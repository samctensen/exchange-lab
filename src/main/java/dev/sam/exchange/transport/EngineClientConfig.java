package dev.sam.exchange.transport;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;

record EngineClientConfig(Path aeronDirectory, ClientConfig clientConfig) {
  static String help() {
    return """
        Usage: AeronEngineClient [options]
        Submit two demo orders and print their results.
        Start AeronEngineServer before this client.

          --aeron-directory=path  Existing engine Media Driver directory
                                  (default: java.io.tmpdir/exchange-lab-aeron)
          --timeout-ms=n          Timeout for each send and reply wait in milliseconds
                                  (default: 5000; positive and representable in nanoseconds)
          --max-attempts=n        Total send attempts, including the first (default: 3; minimum: 1)
          --help                  Print this help and exit; use by itself
        """;
  }

  static EngineClientConfig parse(String[] args) {
    Path aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron");
    Duration timeout = ClientConfig.defaults().timeout();
    int maxAttempts = ClientConfig.defaults().maxAttempts();
    var seen = new HashSet<String>();
    for (String arg : args) {
      String[] parts = arg.split("=", 2);
      if (!seen.add(parts[0])) {
        throw new IllegalArgumentException("Duplicate client argument: " + parts[0]);
      }
      switch (parts[0]) {
        case "--aeron-directory" -> {
          String value = argumentValue(parts);
          if (value.isBlank()) {
            throw new IllegalArgumentException("--aeron-directory must not be blank");
          }
          try {
            aeronDirectory = Path.of(value);
          } catch (InvalidPathException error) {
            throw new IllegalArgumentException("--aeron-directory must be a valid path", error);
          }
        }
        case "--timeout-ms" -> timeout = timeoutValue(parts);
        case "--max-attempts" -> {
          String value = argumentValue(parts);
          try {
            maxAttempts = Integer.parseInt(value);
          } catch (NumberFormatException error) {
            throw new IllegalArgumentException("--max-attempts must be a 32-bit integer: " + value, error);
          }
          if (maxAttempts < 1) {
            throw new IllegalArgumentException("--max-attempts must be positive");
          }
        }
        default -> throw new IllegalArgumentException("Unknown client argument: " + parts[0]);
      }
    }
    return new EngineClientConfig(aeronDirectory, new ClientConfig(timeout, maxAttempts));
  }

  private static Duration timeoutValue(String[] parts) {
    String value = argumentValue(parts);
    long milliseconds;
    try {
      milliseconds = Long.parseLong(value);
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException("--timeout-ms must be a 64-bit integer: " + value, error);
    }
    if (milliseconds < 1 || milliseconds > Long.MAX_VALUE / 1_000_000) {
      throw new IllegalArgumentException("--timeout-ms must be positive and representable in nanoseconds");
    }
    return Duration.ofMillis(milliseconds);
  }

  private static String argumentValue(String[] parts) {
    if (parts.length != 2) {
      throw new IllegalArgumentException("Expected " + parts[0] + "=value");
    }
    return parts[1];
  }
}
