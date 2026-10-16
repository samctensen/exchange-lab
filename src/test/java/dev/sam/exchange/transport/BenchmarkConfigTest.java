package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BenchmarkConfigTest {
  @Test
  void preservesDefaultsWhenOnlyNamedOptionsAreProvided() {
    BenchmarkConfig defaults = BenchmarkConfig.parse(new String[0]);
    assertEquals(500, defaults.warmupCount());
    assertEquals(2_000, defaults.sampleCount());
    assertEquals(1, defaults.maxInFlight());
    assertEquals("sleep", defaults.idleMode());
    assertEquals(Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron"), defaults.aeronDirectory());
    assertFalse(defaults.archiveCounters());

    BenchmarkConfig configured = BenchmarkConfig
        .parse(new String[]{"--aeron-directory=custom driver", "--archive-counters"});
    assertEquals(500, configured.warmupCount());
    assertEquals(2_000, configured.sampleCount());
    assertEquals(1, configured.maxInFlight());
    assertEquals("sleep", configured.idleMode());
    assertEquals(Path.of("custom driver"), configured.aeronDirectory());
    assertTrue(configured.archiveCounters());
  }

  @Test
  void acceptsTheCountLimitsWithoutConnecting() {
    BenchmarkConfig config = BenchmarkConfig.parse(new String[]{"1000000", "1000000"});
    assertEquals(1_000_000, config.warmupCount());
    assertEquals(1_000_000, config.sampleCount());
    assertEquals(0, BenchmarkConfig.parse(new String[]{"0", "1"}).warmupCount());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--aeron-directory=", "--aeron-directory=   ", "--aeron-directory=bad\u0000path"})
  void rejectsInvalidDirectoriesBeforeConnecting(String option) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> AeronLatencyBenchmark.main(new String[]{"0", "1", option}));
    assertTrue(failure.getMessage().contains("--aeron-directory"), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown", "--aeron-directory", "--archive-counters=true"})
  void rejectsUnknownOrMalformedNamedOptions(String option) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> AeronLatencyBenchmark.main(new String[]{"0", "1", option}));
    assertTrue(failure.getMessage().contains("Unknown option"), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--aeron-directory=driver", "--archive-counters"})
  void rejectsDuplicateNamedOptions(String option) {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> AeronLatencyBenchmark.main(new String[]{option, "0", option, "1"}));
    assertTrue(failure.getMessage().contains("Duplicate option"), failure.getMessage());
  }
}
