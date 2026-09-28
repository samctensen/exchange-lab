package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EngineStageTimingsTest {
  @Test
  void skipsWarmupCapsSamplesAndReportsDurationsFromTheSameRequests() {
    EngineStageTimings timings = new EngineStageTimings(1, 2);
    timings.record(0, 1_000_000, 2_000_000, 3_000_000, 4_000_000);
    timings.record(10_000, 30_000, 60_000, 100_000, 150_000);
    timings.record(200_000, 240_000, 300_000, 380_000, 480_000);
    timings.record(0, 1_000_000, 2_000_000, 3_000_000, 4_000_000);

    String report = timings.summarize();
    assertTrue(report.contains("Stage timing: 2/2 samples, skipped 1/1 completed logged requests"), report);
    assertTrue(report.contains("Log offer: mean=30.000 p50=20.000 p99=40.000 max=40.000 us"), report);
    assertTrue(report.contains("Recording observation: mean=45.000 p50=30.000 p99=60.000 max=60.000 us"), report);
    assertTrue(report.contains("Process and encode: mean=60.000 p50=40.000 p99=80.000 max=80.000 us"), report);
    assertTrue(report.contains("Reply offer: mean=75.000 p50=50.000 p99=100.000 max=100.000 us"), report);
    assertTrue(report.contains("Server total: mean=210.000 p50=140.000 p99=280.000 max=280.000 us"), report);
  }

  @Test
  void incompleteWarmupDoesNotProduceZeroValuedLatencyStatistics() {
    EngineStageTimings timings = new EngineStageTimings(2, 3);
    timings.record(0, 1, 2, 3, 4);
    String report = timings.summarize();
    assertTrue(report.contains("Stage timing: 0/3 samples, skipped 1/2 completed logged requests"), report);
    assertFalse(report.contains("p50="), report);
  }

  @Test
  void partiallyFilledBufferSummarizesOnlyCompletedSamples() {
    EngineStageTimings timings = new EngineStageTimings(0, 3);
    timings.record(0, 1_000, 3_000, 6_000, 10_000);
    String report = timings.summarize();
    assertTrue(report.contains("Stage timing: 1/3 samples"), report);
    assertTrue(report.contains("Server total: mean=10.000 p50=10.000 p99=10.000 max=10.000 us"), report);
  }

  @ParameterizedTest
  @CsvSource({"-1, 1", "0, 0", "0, -1", "0, 1000001"})
  void rejectsInvalidOrExcessiveSampleCounts(int warmup, int samples) {
    assertThrows(IllegalArgumentException.class, () -> new EngineStageTimings(warmup, samples));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "1,", "1,2,3", "oops,2", "-1,2", "0,0", "0,1000001"})
  void rejectsInvalidTimingFlagsBeforeStartingTheServer(String counts) {
    assertThrows(IllegalArgumentException.class,
        () -> AeronEngineServer.main(new String[]{"--stage-timing=" + counts}));
  }
}
