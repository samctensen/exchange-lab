package dev.sam.exchange.transport;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashSet;

record EngineClientConfig(Path aeronDirectory) {
  static String help() {
    return """
        Usage: AeronEngineClient [options]
        Submit two demo orders and print their results.
        Start AeronEngineServer before this client.

          --aeron-directory=path  Existing engine Media Driver directory
                                  (default: java.io.tmpdir/exchange-lab-aeron)
          --help                  Print this help and exit; use by itself
        """;
  }

  static EngineClientConfig parse(String[] args) {
    Path aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron");
    var seen = new HashSet<String>();
    for (String arg : args) {
      String[] parts = arg.split("=", 2);
      if (!seen.add(parts[0])) {
        throw new IllegalArgumentException("Duplicate client argument: " + parts[0]);
      }
      switch (parts[0]) {
        case "--aeron-directory" -> {
          if (parts.length != 2) {
            throw new IllegalArgumentException("Expected --aeron-directory=path");
          }
          if (parts[1].isBlank()) {
            throw new IllegalArgumentException("--aeron-directory must not be blank");
          }
          try {
            aeronDirectory = Path.of(parts[1]);
          } catch (InvalidPathException error) {
            throw new IllegalArgumentException("--aeron-directory must be a valid path", error);
          }
        }
        default -> throw new IllegalArgumentException("Unknown client argument: " + parts[0]);
      }
    }
    return new EngineClientConfig(aeronDirectory);
  }
}
