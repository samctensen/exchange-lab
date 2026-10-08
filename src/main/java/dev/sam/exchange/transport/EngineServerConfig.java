package dev.sam.exchange.transport;

import java.nio.file.Path;

record EngineServerConfig(Path archiveDirectory, boolean quiet, int logWindow, int stageWarmup, int stageSamples) {
  private static final String USAGE = "Usage: AeronEngineServer [archiveDirectory] [--quiet] "
      + "[--stage-timing=warmupCount,sampleCount] [--log-window=count]";

  static EngineServerConfig parse(String[] args) {
    Path archiveDirectory = null;
    boolean quiet = false;
    Integer logWindow = null;
    int stageWarmup = 0;
    int stageSamples = 0;
    for (String arg : args) {
      if ("--quiet".equals(arg)) {
        if (quiet) {
          throw new IllegalArgumentException("Duplicate option: --quiet");
        }
        quiet = true;
      } else if (arg.startsWith("--log-window=")) {
        if (logWindow != null) {
          throw new IllegalArgumentException("Duplicate option: --log-window");
        }
        logWindow = parseInteger("--log-window", arg.substring("--log-window=".length()));
        if (logWindow < 1) {
          throw new IllegalArgumentException("--log-window must be positive: " + logWindow);
        }
      } else if (arg.startsWith("--stage-timing=")) {
        if (stageSamples != 0) {
          throw new IllegalArgumentException("Duplicate option: --stage-timing");
        }
        String[] counts = arg.substring("--stage-timing=".length()).split(",", -1);
        if (counts.length != 2) {
          throw new IllegalArgumentException("Usage: --stage-timing=warmupCount,sampleCount");
        }
        stageWarmup = parseInteger("--stage-timing warmupCount", counts[0]);
        stageSamples = parseInteger("--stage-timing sampleCount", counts[1]);
        if (stageWarmup < 0 || stageSamples < 1 || stageSamples > 1_000_000) {
          throw new IllegalArgumentException("--stage-timing requires warmup >= 0 and samples between 1 and 1000000");
        }
      } else if (arg.startsWith("--")) {
        throw new IllegalArgumentException("Unknown argument: " + arg + "\n" + USAGE);
      } else if (archiveDirectory != null) {
        throw new IllegalArgumentException("Only one archive directory is allowed; unexpected argument: " + arg);
      } else {
        archiveDirectory = Path.of(arg);
      }
    }
    return new EngineServerConfig(archiveDirectory == null ? Path.of("data", "archive") : archiveDirectory, quiet,
        logWindow == null ? AeronEngineAgent.DEFAULT_LOG_WINDOW : logWindow, stageWarmup, stageSamples);
  }

  private static int parseInteger(String option, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(option + " requires a 32-bit integer: '" + value + "'", e);
    }
  }
}
