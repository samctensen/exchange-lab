package dev.sam.exchange.gateway;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import dev.sam.exchange.transport.AeronRequestClient;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcGatewayServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    // Connect to the media driver owned by the server.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // Send requests on stream 1 and receive correlated responses on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {

      AeronRequestClient client = new AeronRequestClient(publication, replies);

      try (EngineGateway gateway = new EngineGateway(client, 128)) {
        gateway.start();

        Server server = ServerBuilder.forPort(50051)
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

          System.out.println("gRPC gateway listening on port " + server.getPort());

          // Keep main inside the resource blocks while the server runs.
          server.awaitTermination();
        } finally {
          // Also stop gRPC if main exits because of an exception.
          server.shutdownNow();
        }
      }
    }
  }
}
