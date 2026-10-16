package dev.sam.exchange.transport;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

record BenchmarkConfig(int warmupCount, int sampleCount, int maxInFlight, String idleMode, Path aeronDirectory,
    boolean archiveCounters) {
  static final int MAX_COUNT = 1_000_000;

  static BenchmarkConfig parse(String[] args) {
    List<String> positional = new ArrayList<>(4);
    Path aeronDirectory = null;
    boolean archiveCounters = false;
    for (String arg : args) {
      if ("--archive-counters".equals(arg)) {
        if (archiveCounters)
          throw new IllegalArgumentException("Duplicate option: --archive-counters");
        archiveCounters = true;
      } else if (arg.startsWith("--aeron-directory=")) {
        if (aeronDirectory != null)
          throw new IllegalArgumentException("Duplicate option: --aeron-directory");
        String value = arg.substring("--aeron-directory=".length());
        if (value.isBlank())
          throw new IllegalArgumentException("--aeron-directory must not be blank");
        try {
          aeronDirectory = Path.of(value);
        } catch (InvalidPathException failure) {
          throw new IllegalArgumentException("--aeron-directory requires a valid path", failure);
        }
      } else if (arg.startsWith("--")) {
        throw new IllegalArgumentException("Unknown option: " + arg);
      } else {
        if (positional.size() == 4)
          throw new IllegalArgumentException(
              "Expected at most four positional arguments: warmupCount sampleCount maxInFlight sleep|spin");
        positional.add(arg);
      }
    }

    int warmupCount = positional.size() > 0 ? Integer.parseInt(positional.get(0)) : 500;
    int sampleCount = positional.size() > 1 ? Integer.parseInt(positional.get(1)) : 2_000;
    int maxInFlight = positional.size() > 2 ? Integer.parseInt(positional.get(2)) : 1;
    if (warmupCount < 0 || warmupCount > MAX_COUNT || sampleCount < 1 || sampleCount > MAX_COUNT || maxInFlight < 1) {
      throw new IllegalArgumentException("Warmup count must be between 0 and " + MAX_COUNT
          + "; sample count must be between 1 and " + MAX_COUNT + "; max in flight must be positive");
    }
    String idleMode = positional.size() > 3 ? positional.get(3) : "sleep";
    if (!idleMode.equals("sleep") && !idleMode.equals("spin"))
      throw new IllegalArgumentException("Client idle strategy must be sleep or spin: " + idleMode);
    if (aeronDirectory == null)
      aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron");
    return new BenchmarkConfig(warmupCount, sampleCount, maxInFlight, idleMode, aeronDirectory, archiveCounters);
  }
}
