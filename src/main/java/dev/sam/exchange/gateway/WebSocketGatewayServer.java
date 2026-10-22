package dev.sam.exchange.gateway;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

/** Standalone local WebSocket service. Start AeronEngineServer first. */
public final class WebSocketGatewayServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    if (Arrays.asList(args).contains("--help")) {
      if (args.length != 1)
        throw new IllegalArgumentException("--help must be used alone");
      System.out.println("""
          Usage: WebSocketGatewayServer [options]
          Local demo endpoint: ws://127.0.0.1:8080/orders (no authentication or TLS)
          Start AeronEngineServer before this gateway.

            --port=n             Listen port, 0..65535; 0 selects an available port (default: 8080)
            --queue-capacity=n   Maximum queued requests, positive integer (default: 128)
            --max-in-flight=n    Maximum active requests, positive integer (default: 8)
            --max-connections=n  Maximum accepted TCP connections (default: 128)
            --aeron-directory=p  Existing engine Media Driver directory
                                 (default: java.io.tmpdir/exchange-lab-aeron)
            --timeout-ms=n       Timeout for each send and reply wait in milliseconds (default: 5000)
            --max-attempts=n     Total send attempts, including the first (default: 3; minimum: 1)
            --diagnostics        Print aggregate lifetime counters after shutdown drain (default: off)
            --help               Print this help and exit; use by itself
          """);
      return;
    }
    Config config = Config.parse(args);
    GatewayDiagnostics diagnostics = config.diagnostics() ? new GatewayDiagnostics() : null;
    Thread shutdownHook = null;
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectory()));
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication requests = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(), 1);
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(requests, replies, config.clientConfig()),
            config.queueCapacity(), config.maxInFlight(), diagnostics);
        WebSocketGateway server = new WebSocketGateway(gateway, config.port(), config.webSocketLimits())) {
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
      // Resource closure above drains the gateway before the lifetime counters are reported.
      if (diagnostics != null)
        System.out.print(diagnostics.summarize());
      if (shutdownHook != null) {
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException shutdownInProgress) {
          // The running hook is waiting for this thread to finish resource cleanup.
        }
      }
    }
  }

  record Config(int port, int queueCapacity, int maxInFlight, String aeronDirectory, ClientConfig clientConfig,
      boolean diagnostics, WebSocketLimits webSocketLimits) {
    Config(int port, int queueCapacity, int maxInFlight, String aeronDirectory, ClientConfig clientConfig,
        boolean diagnostics) {
      this(port, queueCapacity, maxInFlight, aeronDirectory, clientConfig, diagnostics, WebSocketLimits.defaults());
    }

    Config(int port, int queueCapacity, int maxInFlight, String aeronDirectory, ClientConfig clientConfig) {
      this(port, queueCapacity, maxInFlight, aeronDirectory, clientConfig, false);
    }

    Config(int port, int queueCapacity, int maxInFlight) {
      this(port, queueCapacity, maxInFlight, defaultAeronDirectory(), ClientConfig.defaults());
    }

    Config {
      if (port < 0 || port > 65535)
        throw new IllegalArgumentException("--port must be between 0 and 65535");
      if (queueCapacity < 1)
        throw new IllegalArgumentException("--queue-capacity must be positive");
      if (maxInFlight < 1)
        throw new IllegalArgumentException("--max-in-flight must be positive");
      if (aeronDirectory == null || aeronDirectory.isBlank())
        throw new IllegalArgumentException("--aeron-directory must not be blank");
      try {
        Path.of(aeronDirectory);
      } catch (IllegalArgumentException error) {
        throw new IllegalArgumentException("--aeron-directory must be a valid path", error);
      }
      if (clientConfig == null)
        throw new IllegalArgumentException("clientConfig must not be null");
      if (webSocketLimits == null)
        throw new IllegalArgumentException("webSocketLimits must not be null");
    }

    static Config parse(String[] args) {
      int port = 8080;
      int queueCapacity = 128;
      int maxInFlight = 8;
      String aeronDirectory = defaultAeronDirectory();
      Duration timeout = ClientConfig.defaults().timeout();
      int maxAttempts = ClientConfig.defaults().maxAttempts();
      boolean diagnostics = false;
      WebSocketLimits defaults = WebSocketLimits.defaults();
      int maxConnections = defaults.maxConnections();
      var seen = new HashSet<String>();
      for (String arg : args) {
        String[] parts = arg.split("=", 2);
        if (!seen.add(parts[0]))
          throw new IllegalArgumentException("Duplicate gateway argument: " + parts[0]);
        switch (parts[0]) {
          case "--port" -> port = integerValue(parts);
          case "--queue-capacity" -> queueCapacity = integerValue(parts);
          case "--max-in-flight" -> maxInFlight = integerValue(parts);
          case "--max-connections" -> maxConnections = positiveIntegerValue(parts);
          case "--aeron-directory" -> aeronDirectory = argumentValue(parts);
          case "--timeout-ms" -> timeout = timeoutValue(parts);
          case "--diagnostics" -> {
            if (parts.length != 1)
              throw new IllegalArgumentException("--diagnostics does not take a value");
            diagnostics = true;
          }
          case "--max-attempts" -> {
            maxAttempts = integerValue(parts);
            if (maxAttempts < 1)
              throw new IllegalArgumentException("--max-attempts must be positive");
          }
          default -> throw new IllegalArgumentException("Unknown gateway argument: " + parts[0]);
        }
      }
      return new Config(port, queueCapacity, maxInFlight, aeronDirectory, new ClientConfig(timeout, maxAttempts),
          diagnostics,
          new WebSocketLimits(maxConnections, defaults.maxPendingPerConnection(), defaults.maxMessageBytes(),
              defaults.maxResponseBytes(), defaults.readIdleTimeoutMillis(), defaults.handshakeTimeoutMillis()));
    }

    private static Duration timeoutValue(String[] parts) {
      String value = argumentValue(parts);
      long milliseconds;
      try {
        milliseconds = Long.parseLong(value);
      } catch (NumberFormatException error) {
        throw new IllegalArgumentException("--timeout-ms must be a 64-bit integer: " + value, error);
      }
      if (milliseconds < 1 || milliseconds > Long.MAX_VALUE / 1_000_000)
        throw new IllegalArgumentException("--timeout-ms must be positive and representable in nanoseconds");
      return Duration.ofMillis(milliseconds);
    }

    private static int integerValue(String[] parts) {
      String value = argumentValue(parts);
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException error) {
        throw new IllegalArgumentException(parts[0] + " must be a 32-bit integer: " + value, error);
      }
    }

    private static int positiveIntegerValue(String[] parts) {
      int value = integerValue(parts);
      if (value < 1)
        throw new IllegalArgumentException(parts[0] + " must be positive");
      return value;
    }

    private static String argumentValue(String[] parts) {
      if (parts.length != 2)
        throw new IllegalArgumentException("Expected " + parts[0] + "=value");
      return parts[1];
    }

    private static String defaultAeronDirectory() {
      return Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();
    }
  }
}
