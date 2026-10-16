package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(10)
class BenchmarkBoundsTest {
  @Test
  void rejectsOversizedMeasurementBeforeAllocatingOrAccessingClient() {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> AeronLatencyBenchmark.measure(null, 1_000_001, 1, null, null));
    assertTrue(failure.getMessage().contains("1000000"), failure.getMessage());
  }

  @ParameterizedTest
  @CsvSource({"1000001,1", "0,1000001", "2147483647,1", "0,2147483647"})
  void rejectsOversizedCliCountsOnASmallHeapBeforeConnecting(String warmup, String samples, @TempDir Path tempDir)
      throws Exception {
    Path log = tempDir.resolve("benchmark.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx32m", "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp",
        classpath, AeronLatencyBenchmark.class.getName(), warmup, samples));
    Process benchmark = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(benchmark.waitFor(3, TimeUnit.SECONDS), "Invalid counts must fail before looking for a driver");
      String output = Files.readString(log);
      assertTrue(benchmark.exitValue() != 0, output);
      assertTrue(output.contains("IllegalArgumentException"), output);
      assertTrue(output.contains("1000000"), output);
      assertFalse(output.contains("OutOfMemoryError"), output);
      assertFalse(output.contains("DriverTimeoutException"), output);
      assertFalse(output.contains("Samples:"), output);
      assertFalse(Files.exists(tempDir.resolve("exchange-lab-aeron")));
    } finally {
      if (benchmark.isAlive()) {
        benchmark.destroyForcibly();
        assertTrue(benchmark.waitFor(3, TimeUnit.SECONDS));
      }
    }
  }
}
