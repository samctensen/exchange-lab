package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EngineServerConfigTest {
  @Test
  void defaultsToPersistentArchiveWithResultLoggingAndNoStageTiming() {
    EngineServerConfig config = EngineServerConfig.parse(new String[0]);

    assertEquals(Path.of("data", "archive"), config.archiveDirectory());
    assertFalse(config.quiet());
    assertEquals(8, config.logWindow());
    assertEquals(0, config.stageWarmup());
    assertEquals(0, config.stageSamples());
    assertEquals(new ReplyDeliveryConfig(64, 65536, Duration.ofSeconds(5)), config.replyDelivery());
    assertFalse(config.replyStats());
  }

  @Test
  void acceptsArchiveBetweenFlagsWithoutCreatingIt(@TempDir Path tempDir) {
    Path archive = tempDir.resolve("custom archive");
    EngineServerConfig config = EngineServerConfig
        .parse(new String[]{"--quiet", "--stage-timing=3,17", archive.toString(), "--log-window=1"});

    assertEquals(archive, config.archiveDirectory());
    assertTrue(config.quiet());
    assertEquals(1, config.logWindow());
    assertEquals(3, config.stageWarmup());
    assertEquals(17, config.stageSamples());
    assertFalse(Files.exists(archive));
  }

  @Test
  void acceptsStageTimingAndLogWindowUpperBoundaries() {
    EngineServerConfig config = EngineServerConfig
        .parse(new String[]{"--stage-timing=2147483647,1000000", "--log-window=2147483647"});

    assertEquals(2_147_483_647, config.stageWarmup());
    assertEquals(1_000_000, config.stageSamples());
    assertEquals(2_147_483_647, config.logWindow());
  }

  @Test
  void acceptsReplyPolicyOverridesAndStatistics() {
    EngineServerConfig config = EngineServerConfig
        .parse(new String[]{"--reply-bytes=4096", "--reply-stats", "--reply-timeout-ms=250", "--reply-capacity=2"});

    assertEquals(new ReplyDeliveryConfig(2, 4096, Duration.ofMillis(250)), config.replyDelivery());
    assertTrue(config.replyStats());
  }

  @Test
  void acceptsLargestRepresentableReplyPolicyValues() {
    EngineServerConfig config = EngineServerConfig.parse(new String[]{"--reply-capacity=2147483647",
        "--reply-bytes=9223372036854775807", "--reply-timeout-ms=9223372036854"});

    assertEquals(
        new ReplyDeliveryConfig(2_147_483_647, 9_223_372_036_854_775_807L, Duration.ofMillis(9_223_372_036_854L)),
        config.replyDelivery());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--quiet", "--log-window=4", "--stage-timing=0,1", "--reply-capacity=2", "--reply-bytes=4096",
      "--reply-timeout-ms=250", "--reply-stats"})
  void rejectsDuplicateOptionsWithTheirNames(String option) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{option, option}));

    assertTrue(error.getMessage().contains("Duplicate"), error::getMessage);
    assertTrue(error.getMessage().contains(option.split("=", 2)[0]), error::getMessage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "", "oops", "2147483648"})
  void rejectsInvalidLogWindowsWithTheOptionName(String value) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{"--log-window=" + value}));

    assertTrue(error.getMessage().contains("--log-window"), error::getMessage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"--reply-capacity=0", "--reply-capacity=-1", "--reply-capacity=", "--reply-capacity=oops",
      "--reply-capacity=2147483648", "--reply-bytes=0", "--reply-bytes=-1", "--reply-bytes=", "--reply-bytes=oops",
      "--reply-bytes=9223372036854775808", "--reply-timeout-ms=0", "--reply-timeout-ms=-1", "--reply-timeout-ms=",
      "--reply-timeout-ms=oops", "--reply-timeout-ms=9223372036855", "--reply-timeout-ms=9223372036854775808"})
  void rejectsInvalidReplyPolicyValuesWithTheOptionName(String argument) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{argument}));

    assertTrue(error.getMessage().contains(argument.split("=", 2)[0]), error::getMessage);
    assertFalse(error.getMessage().contains("Unknown argument"), error::getMessage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "1,", ",1", "1,2,3", "oops,2", "0,oops", "-1,2", "0,0", "0,-1", "0,1000001",
      "2147483648,1", "0,2147483648"})
  void rejectsInvalidTimingCountsWithTheOptionName(String counts) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{"--stage-timing=" + counts}));

    assertTrue(error.getMessage().contains("--stage-timing"), error::getMessage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown", "--quiet=true", "--log-window", "--stage-timing", "--reply-capacity",
      "--reply-bytes", "--reply-timeout-ms", "--reply-stats=true"})
  void identifiesUnsupportedArgumentsAndShowsUsage(String argument) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{argument}));

    assertTrue(error.getMessage().contains(argument), error::getMessage);
    assertTrue(error.getMessage().contains("Usage:"), error::getMessage);
  }

  @Test
  void rejectsMultipleArchiveDirectories() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{"first", "second"}));

    assertTrue(error.getMessage().contains("archive"), error::getMessage);
    assertTrue(error.getMessage().contains("second"), error::getMessage);
  }

  @Test
  @Timeout(10)
  void standaloneHelpPrintsOptionsAndDefaultsWithoutCreatingResources(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("help.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Djava.io.tmpdir=" + tempDir, "-cp", classpath, AeronEngineServer.class.getName(), "--help")
        .directory(tempDir.toFile()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(server.waitFor(5, TimeUnit.SECONDS), "Help must exit without starting the server");
      String help = Files.readString(output);
      assertEquals(0, server.exitValue(), help);
      assertTrue(help.contains("Usage: AeronEngineServer"), help);
      assertTrue(help.contains("--quiet"), help);
      assertTrue(help.contains("--log-window=count"), help);
      assertTrue(help.contains("--stage-timing=warmupCount,sampleCount"), help);
      assertTrue(help.contains("--help"), help);
      assertTrue(help.contains("--reply-capacity=count"), help);
      assertTrue(help.contains("--reply-bytes=count"), help);
      assertTrue(help.contains("--reply-timeout-ms=milliseconds"), help);
      assertTrue(help.contains("--reply-stats"), help);
      assertTrue(help.contains("data/archive"), help);
      assertTrue(help.contains("default: 8"), help);
      assertTrue(help.contains("default: 64"), help);
      assertTrue(help.contains("default: 65536"), help);
      assertTrue(help.contains("default: 5000"), help);
      assertTrue(help.contains("disabled"), help);
      assertFalse(Files.exists(tempDir.resolve("data")));
      assertFalse(Files.exists(tempDir.resolve("exchange-lab-aeron")));
    } finally {
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--quiet", "--unknown", "--help", "archive"})
  void rejectsHelpCombinedWithOtherArguments(String other) {
    assertThrows(IllegalArgumentException.class, () -> AeronEngineServer.main(new String[]{"--help", other}));
    assertThrows(IllegalArgumentException.class, () -> AeronEngineServer.main(new String[]{other, "--help"}));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown", "--reply-capacity=0", "--reply-bytes=0", "--reply-timeout-ms=9223372036855"})
  @Timeout(10)
  void rejectsLaterInvalidArgumentBeforeAllocatingTimingBuffersOrCreatingResources(String argument,
      @TempDir Path tempDir) throws Exception {
    Path archive = tempDir.resolve("archive");
    Path output = tempDir.resolve("startup.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    // Timing buffers for a million samples exceed this heap. Argument rejection must happen first.
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx16m",
        "-cp", classpath, AeronEngineServer.class.getName(), archive.toString(), "--stage-timing=0,1000000", argument)
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(server.waitFor(5, TimeUnit.SECONDS), "Invalid arguments must exit without starting the server");
      String error = Files.readString(output);
      assertTrue(server.exitValue() != 0, error);
      assertTrue(error.contains("IllegalArgumentException"), error);
      assertTrue(error.contains(argument.split("=", 2)[0]), error);
      assertFalse(Files.exists(archive));
    } finally {
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }
}
