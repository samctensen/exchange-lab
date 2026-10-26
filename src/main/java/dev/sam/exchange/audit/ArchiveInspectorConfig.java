package dev.sam.exchange.audit;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;

/** Validates arguments before the inspector opens any filesystem or Aeron resources. */
record ArchiveInspectorConfig(Path archive, int levels, Format format) {
  enum Format {
    TEXT, JSON
  }

  ArchiveInspectorConfig {
    Objects.requireNonNull(archive, "archive");
    Objects.requireNonNull(format, "format");
    if (archive.toString().isBlank())
      throw new IllegalArgumentException("--archive must not be blank");
    if (levels < 1 || levels > 1000)
      throw new IllegalArgumentException("--levels must be between 1 and 1000");
  }

  static ArchiveInspectorConfig parse(String[] args) {
    Path archive = null;
    int levels = 10;
    Format format = Format.TEXT;
    var seen = new HashSet<String>();
    for (String arg : args) {
      String[] parts = arg.split("=", 2);
      if (!seen.add(parts[0]))
        throw new IllegalArgumentException("Duplicate inspector argument: " + parts[0]);
      switch (parts[0]) {
        case "--archive" -> {
          String value = value(parts);
          if (value.isBlank())
            throw new IllegalArgumentException("--archive must not be blank");
          try {
            archive = Path.of(value);
          } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("--archive must be a valid path", invalid);
          }
        }
        case "--levels" -> {
          try {
            levels = Integer.parseInt(value(parts));
          } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("--levels must be an integer between 1 and 1000", invalid);
          }
        }
        case "--format" -> format = switch (value(parts)) {
          case "text" -> Format.TEXT;
          case "json" -> Format.JSON;
          default -> throw new IllegalArgumentException("--format must be text or json");
        };
        default -> throw new IllegalArgumentException("Unknown inspector argument: " + parts[0]);
      }
    }
    if (archive == null)
      throw new IllegalArgumentException("--archive=path is required");
    return new ArchiveInspectorConfig(archive, levels, format);
  }

  private static String value(String[] parts) {
    if (parts.length != 2)
      throw new IllegalArgumentException("Expected " + parts[0] + "=value");
    return parts[1];
  }
}
