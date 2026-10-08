package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
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

  @ParameterizedTest
  @ValueSource(strings = {"--quiet", "--log-window=4", "--stage-timing=0,1"})
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
  @ValueSource(strings = {"", "1", "1,", ",1", "1,2,3", "oops,2", "0,oops", "-1,2", "0,0", "0,-1", "0,1000001",
      "2147483648,1", "0,2147483648"})
  void rejectsInvalidTimingCountsWithTheOptionName(String counts) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EngineServerConfig.parse(new String[]{"--stage-timing=" + counts}));

    assertTrue(error.getMessage().contains("--stage-timing"), error::getMessage);
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown", "--quiet=true", "--log-window", "--stage-timing"})
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
  void rejectsLaterInvalidArgumentBeforeAllocatingTimingBuffersOrCreatingResources(@TempDir Path tempDir)
      throws Exception {
    Path archive = tempDir.resolve("archive");
    Path output = tempDir.resolve("startup.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    // Timing buffers for a million samples exceed this heap. Argument rejection must happen first.
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx16m",
        "-cp", classpath, AeronEngineServer.class.getName(), archive.toString(), "--stage-timing=0,1000000",
        "--unknown").redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(server.waitFor(5, TimeUnit.SECONDS), "Invalid arguments must exit without starting the server");
      String error = Files.readString(output);
      assertTrue(server.exitValue() != 0, error);
      assertTrue(error.contains("IllegalArgumentException"), error);
      assertTrue(error.contains("--unknown"), error);
      assertFalse(Files.exists(archive));
    } finally {
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }
}
