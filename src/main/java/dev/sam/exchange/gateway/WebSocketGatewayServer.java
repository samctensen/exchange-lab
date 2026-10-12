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
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication requests = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(), 1);
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
    Config {
      if (port < 0 || port > 65535)
        throw new IllegalArgumentException("--port must be between 0 and 65535");
      if (queueCapacity < 1)
        throw new IllegalArgumentException("--queue-capacity must be positive");
      if (maxInFlight < 1)
        throw new IllegalArgumentException("--max-in-flight must be positive");
    }

    static Config parse(String[] args) {
      int port = 8080;
      int queueCapacity = 128;
      int maxInFlight = 8;
      var seen = new HashSet<String>();
      for (String arg : args) {
        String[] parts = arg.split("=", 2);
        if (!seen.add(parts[0]))
          throw new IllegalArgumentException("Duplicate gateway argument: " + parts[0]);
        switch (parts[0]) {
          case "--port" -> port = integerValue(parts);
          case "--queue-capacity" -> queueCapacity = integerValue(parts);
          case "--max-in-flight" -> maxInFlight = integerValue(parts);
          default -> throw new IllegalArgumentException("Unknown gateway argument: " + parts[0]);
        }
      }
      return new Config(port, queueCapacity, maxInFlight);
    }

    private static int integerValue(String[] parts) {
      if (parts.length != 2)
        throw new IllegalArgumentException("Expected " + parts[0] + "=n");
      try {
        return Integer.parseInt(parts[1]);
      } catch (NumberFormatException error) {
        throw new IllegalArgumentException(parts[0] + " must be a 32-bit integer: " + parts[1], error);
      }
    }
  }
}
