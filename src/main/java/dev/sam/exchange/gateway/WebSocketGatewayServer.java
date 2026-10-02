package dev.sam.exchange.gateway;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;

import dev.sam.exchange.transport.AeronRequestClient;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

/** Standalone local WebSocket service. Start AeronEngineServer first. */
public final class WebSocketGatewayServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    Config config = Config.parse(args);
    String directory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();
    Thread shutdownHook = null;
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory));
        Publication requests = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(requests, replies), config.queueCapacity(),
            config.maxInFlight());
        WebSocketGateway server = new WebSocketGateway(gateway, config.port())) {
      gateway.start();
      server.start();
      Thread mainThread = Thread.currentThread();
      shutdownHook = new Thread(() -> {
        // Disconnecting clients does not undo accepted commands. Main drains the gateway before closing Aeron.
        server.close();
        try {
          mainThread.join();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }, "websocket-gateway-shutdown");
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      System.out.println("WebSocket gateway listening on ws://127.0.0.1:" + server.port() + "/orders");
      System.out.println("Local demo only: no authentication or TLS");
      System.out.println("Gateway queue capacity: " + config.queueCapacity());
      System.out.println("Gateway max in flight: " + config.maxInFlight());
      server.awaitClose();
    } finally {
      if (shutdownHook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException shutdownInProgress) {
          // The running hook is waiting for this thread to finish resource cleanup.
        }
      }
    }
  }

  record Config(int port, int queueCapacity, int maxInFlight) {
    static Config parse(String[] args) {
      int port = 8080;
      int queueCapacity = 128;
      int maxInFlight = 8;
      var seen = new HashSet<String>();
      for (String arg : args) {
        String[] parts = arg.split("=", 2);
        if (!seen.add(parts[0]))
          throw new IllegalArgumentException("Duplicate gateway argument: " + parts[0]);
        if (parts.length != 2)
          throw new IllegalArgumentException("Expected --port=n, --queue-capacity=n or --max-in-flight=n");
        int value = Integer.parseInt(parts[1]);
        switch (parts[0]) {
          case "--port" -> port = value;
          case "--queue-capacity" -> queueCapacity = value;
          case "--max-in-flight" -> maxInFlight = value;
          default -> throw new IllegalArgumentException("Unknown gateway argument: " + parts[0]);
        }
      }
      if (port < 0 || port > 65535 || queueCapacity < 1 || maxInFlight < 1)
        throw new IllegalArgumentException("Port must be 0..65535; queue capacity and max in flight must be positive");
      return new Config(port, queueCapacity, maxInFlight);
    }
  }
}
