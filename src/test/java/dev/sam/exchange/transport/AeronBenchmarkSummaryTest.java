package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AeronBenchmarkSummaryTest {
  @ParameterizedTest
  @ValueSource(longs = {-1, Long.MIN_VALUE})
  void rejectsNegativeLatenciesInsteadOfReportingMisleadingPercentiles(long invalid) {
    assertThrows(IllegalArgumentException.class,
        () -> AeronLatencyBenchmark.summarize(new long[]{1000, invalid, 2000}));
  }

  @Test
  void allowsZeroResolutionSamplesAndPreservesAcquisitionOrder() {
    long[] samples = {2000, 0, 1000};
    String report = AeronLatencyBenchmark.summarize(samples);
    assertTrue(report.contains("p50: 1.000 us"));
    assertTrue(report.contains("max: 2.000 us"));
    assertArrayEquals(new long[]{2000, 0, 1000}, samples);
  }
}
