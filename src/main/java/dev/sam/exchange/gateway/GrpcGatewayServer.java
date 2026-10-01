package dev.sam.exchange.gateway;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import dev.sam.exchange.transport.AeronRequestClient;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcGatewayServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    Config config = Config.parse(args);
    GatewayDiagnostics diagnostics = config.diagnostics() ? new GatewayDiagnostics() : null;
    // Connect to the media driver owned by the server.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // Send requests on stream 1 and receive correlated responses on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {

      AeronRequestClient client = new AeronRequestClient(publication, replies);

      try (EngineGateway gateway = new EngineGateway(client, config.queueCapacity(), config.maxInFlight(),
          diagnostics)) {
        gateway.start();

        Server server = ServerBuilder.forPort(config.port())
            .addService(new GrpcExchangeService(gateway, new GrpcCommandMapper())).build();

        try {
          server.start();

          Thread mainThread = Thread.currentThread();

          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Stop accepting new RPCs; existing calls can still finish.
            server.shutdown();

            try {
              // Give existing RPCs a grace period.
              if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                server.shutdownNow();
              }

              // Keep the JVM alive while main closes the gateway and Aeron.
              mainThread.join();
            } catch (InterruptedException e) {
              server.shutdownNow();
              Thread.currentThread().interrupt();
            }
          }, "grpc-gateway-shutdown"));

          System.out.println("Gateway max in flight: " + config.maxInFlight());
          System.out.println("Gateway queue capacity: " + config.queueCapacity());
          System.out.println("gRPC gateway listening on port " + server.getPort());

          // Keep main inside the resource blocks while the server runs.
          server.awaitTermination();
        } finally {
          // Also stop gRPC if main exits because of an exception.
          server.shutdownNow();
        }
      } finally {
        if (diagnostics != null)
          System.out.print(diagnostics.summarize());
      }
    }
  }

  private record Config(int port, int queueCapacity, int maxInFlight, boolean diagnostics) {
    static Config parse(String[] args) {
      int port = 50051;
      int queueCapacity = 128;
      int maxInFlight = 8;
      boolean diagnostics = false;
      var seen = new HashSet<String>();
      for (String arg : args) {
        String[] parts = arg.split("=", 2);
        if (!seen.add(parts[0]))
          throw new IllegalArgumentException("Duplicate gateway argument: " + parts[0]);
        if (arg.equals("--diagnostics")) {
          diagnostics = true;
          continue;
        }
        if (parts.length != 2)
          throw new IllegalArgumentException(
              "Expected --port=n, --queue-capacity=n, --max-in-flight=n or --diagnostics");
        int value = Integer.parseInt(parts[1]);
        switch (parts[0]) {
          case "--port" -> port = value;
          case "--queue-capacity" -> queueCapacity = value;
          case "--max-in-flight" -> maxInFlight = value;
          default -> throw new IllegalArgumentException("Unknown gateway argument: " + parts[0]);
        }
      }
      if (port < 0 || port > 65535 || queueCapacity < 1 || maxInFlight < 1) {
        throw new IllegalArgumentException("Port must be 0..65535; queue capacity and max in flight must be positive");
      }
      return new Config(port, queueCapacity, maxInFlight, diagnostics);
    }
  }
}
