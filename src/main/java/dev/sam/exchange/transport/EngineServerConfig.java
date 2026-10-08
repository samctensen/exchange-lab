package dev.sam.exchange.transport;

import java.nio.file.Path;
import java.time.Duration;

record EngineServerConfig(Path archiveDirectory, boolean quiet, int logWindow, int stageWarmup, int stageSamples,
    ReplyDeliveryConfig replyDelivery, boolean replyStats) {
  private static final String USAGE = "Usage: AeronEngineServer [archiveDirectory] [--quiet] "
      + "[--stage-timing=warmupCount,sampleCount] [--log-window=count]\n"
      + "       [--reply-capacity=count] [--reply-bytes=count] [--reply-timeout-ms=milliseconds] [--reply-stats]\n"
      + "       AeronEngineServer --help";

  static String help() {
    ReplyDeliveryConfig defaults = ReplyDeliveryConfig.defaults();
    return USAGE + "\n\n" + """
        Options:
          archiveDirectory                      Persistent archive location (default: data/archive).
          --quiet                               Suppress per-result logging (default: result logging enabled).
          --log-window=count                    Request log window (default: %d; count >= 1).
          --stage-timing=warmupCount,sampleCount Collect stage timing (default: disabled).
                                                Warmup must be >= 0; samples must be 1..1000000.
          --reply-capacity=count                Maximum waiting replies per client (default: %d; count >= 1).
          --reply-bytes=count                   Maximum waiting reply bytes per client (default: %d; count >= 1).
          --reply-timeout-ms=milliseconds       Reply delivery deadline (default: %d; milliseconds >= 1).
                                                Must fit in nanoseconds; independent of the recording deadline.
          --reply-stats                        Report delivery counts at shutdown (default: disabled).
          --help                                Show this help; use by itself.
        """.formatted(AeronEngineAgent.DEFAULT_LOG_WINDOW, defaults.maxReplies(), defaults.maxBytes(),
        defaults.timeout().toMillis());
  }

  static EngineServerConfig parse(String[] args) {
    Path archiveDirectory = null;
    boolean quiet = false;
    Integer logWindow = null;
    int stageWarmup = 0;
    int stageSamples = 0;
    Integer replyCapacity = null;
    Long replyBytes = null;
    Long replyTimeoutMillis = null;
    boolean replyStats = false;
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
      } else if (arg.startsWith("--reply-capacity=")) {
        if (replyCapacity != null) {
          throw new IllegalArgumentException("Duplicate option: --reply-capacity");
        }
        replyCapacity = parseInteger("--reply-capacity", arg.substring("--reply-capacity=".length()));
        if (replyCapacity < 1) {
          throw new IllegalArgumentException("--reply-capacity must be positive: " + replyCapacity);
        }
      } else if (arg.startsWith("--reply-bytes=")) {
        if (replyBytes != null) {
          throw new IllegalArgumentException("Duplicate option: --reply-bytes");
        }
        replyBytes = parsePositiveLong("--reply-bytes", arg.substring("--reply-bytes=".length()));
      } else if (arg.startsWith("--reply-timeout-ms=")) {
        if (replyTimeoutMillis != null) {
          throw new IllegalArgumentException("Duplicate option: --reply-timeout-ms");
        }
        replyTimeoutMillis = parsePositiveLong("--reply-timeout-ms", arg.substring("--reply-timeout-ms=".length()));
        if (replyTimeoutMillis > Long.MAX_VALUE / 1_000_000) {
          throw new IllegalArgumentException("--reply-timeout-ms must not exceed " + Long.MAX_VALUE / 1_000_000);
        }
      } else if ("--reply-stats".equals(arg)) {
        if (replyStats) {
          throw new IllegalArgumentException("Duplicate option: --reply-stats");
        }
        replyStats = true;
      } else if (arg.startsWith("--")) {
        throw new IllegalArgumentException("Unknown argument: " + arg + "\n" + USAGE);
      } else if (archiveDirectory != null) {
        throw new IllegalArgumentException("Only one archive directory is allowed; unexpected argument: " + arg);
      } else {
        archiveDirectory = Path.of(arg);
      }
    }
    ReplyDeliveryConfig defaults = ReplyDeliveryConfig.defaults();
    ReplyDeliveryConfig replyDelivery = new ReplyDeliveryConfig(
        replyCapacity == null ? defaults.maxReplies() : replyCapacity,
        replyBytes == null ? defaults.maxBytes() : replyBytes,
        replyTimeoutMillis == null ? defaults.timeout() : Duration.ofMillis(replyTimeoutMillis));
    return new EngineServerConfig(archiveDirectory == null ? Path.of("data", "archive") : archiveDirectory, quiet,
        logWindow == null ? AeronEngineAgent.DEFAULT_LOG_WINDOW : logWindow, stageWarmup, stageSamples, replyDelivery,
        replyStats);
  }

  private static int parseInteger(String option, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(option + " requires a 32-bit integer: '" + value + "'", e);
    }
  }

  private static long parsePositiveLong(String option, String value) {
    try {
      long parsed = Long.parseLong(value);
      if (parsed < 1) {
        throw new IllegalArgumentException(option + " must be positive: " + value);
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(option + " requires a positive 64-bit integer: '" + value + "'", e);
    }
  }
}
