package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

@Timeout(20)
class AeronLatencyBenchmarkTest {
  @ParameterizedTest
  @CsvSource({"0, 1", "5, 12"})
  void measuresFreshRequestsAfterWarmupAgainstAQuietServer(int warmup, int samples, @TempDir Path tempDir)
      throws Exception {
    Path archiveDirectory = tempDir.resolve("benchmark-archive");
    Path serverLog = tempDir.resolve("server.log");
    Path benchmarkLog = tempDir.resolve("benchmark.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Process server = startMain(tempDir, serverLog, AeronEngineServer.class, archiveDirectory.toString(), "--quiet");
    Process benchmark = null;
    try {
      awaitOutput(server, serverLog, "Server ready:");
      benchmark = startMain(tempDir, benchmarkLog, AeronLatencyBenchmark.class, Integer.toString(warmup),
          Integer.toString(samples));
      assertTrue(benchmark.waitFor(10, TimeUnit.SECONDS), "Benchmark did not finish");
      String report = Files.readString(benchmarkLog);
      assertEquals(0, benchmark.exitValue(), report);
      assertTrue(report.contains("Warmup: " + warmup + " requests"), report);
      assertTrue(report.contains("Samples: " + samples + " requests"), report);
      assertTrue(report.contains("Attempts per request: 1"), report);
      double p50 = metric(report, "p50");
      double p99 = metric(report, "p99");
      double max = metric(report, "max");
      assertTrue(p50 > 0 && p50 <= p99 && p99 <= max, report);

      assertTrue(server.isAlive(), "The benchmark must leave the caller's server running");

      server.destroy();
      assertTrue(server.waitFor(3, TimeUnit.SECONDS), "Quiet server did not shut down");
      List<CommandRequest> recorded = ArchiveTestSupport.readAll(archiveDirectory);
      assertEquals(warmup + samples, recorded.size(), "Warmup must be sent but excluded from the reported samples");
      assertEquals((long) recorded.size(), recorded.stream().map(CommandRequest::requestId).distinct().count(),
          "Every request must use a new UUID so the benchmark cannot measure cached responses");
      assertTrue(recorded.stream().allMatch(request -> request.command().equals(new CancelOrder(1L))));

      String output = Files.readString(serverLog);
      assertTrue(output.contains("Server ready:"), "Quiet mode must retain startup information");
      assertFalse(output.contains("Result:"), "Per-order result logging must be disabled in quiet mode\n" + output);
      assertFalse(output.contains("Exception"), output);
      assertFalse(Files.exists(driverDirectory), "Server did not clean up its driver");
    } finally {
      stopIfAlive(benchmark);
      stopIfAlive(server);
    }
  }

  @Test
  void rejectsAnUnexpectedResponseWithoutReportingStatistics(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("benchmark-archive");
    Path serverLog = tempDir.resolve("server.log");
    Path benchmarkLog = tempDir.resolve("benchmark.log");
    ArchiveTestSupport.record(archiveDirectory,
        List.of(new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L))));
    Process server = startMain(tempDir, serverLog, AeronEngineServer.class, "--quiet", archiveDirectory.toString());
    Process benchmark = null;
    try {
      awaitOutput(server, serverLog, "Server ready:");
      benchmark = startMain(tempDir, benchmarkLog, AeronLatencyBenchmark.class, "0", "2");
      assertTrue(benchmark.waitFor(5, TimeUnit.SECONDS), "Benchmark did not reject the unexpected response");
      String output = Files.readString(benchmarkLog);
      assertTrue(benchmark.exitValue() != 0, output);
      assertTrue(output.contains("Unexpected benchmark response"), output);
      assertTrue(output.contains("cancelled=true"), output);
      assertFalse(output.contains("p50:"), output);
      assertTrue(server.isAlive(), "A failed benchmark must not terminate its server");
      server.destroy();
      assertTrue(server.waitFor(3, TimeUnit.SECONDS), "Server did not shut down");
      assertEquals(2, ArchiveTestSupport.readAll(archiveDirectory).size(), "Stop after the first unexpected response");
    } finally {
      stopIfAlive(benchmark);
      stopIfAlive(server);
    }
  }

  @Test
  void stopsAfterOneUnansweredAttemptWithoutPrintingPartialStatistics(@TempDir Path tempDir) throws Exception {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    Path log = tempDir.resolve("benchmark.log");
    try (
        MediaDriver driver = MediaDriver.launchEmbedded(
            new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("exchange-lab-aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1)) {
      Process benchmark = startMain(tempDir, log, AeronLatencyBenchmark.class, "0", "2");
      List<CommandRequest> received = new ArrayList<>();
      SbeRequestCodec codec = new SbeRequestCodec();
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
      try {
        while (benchmark.isAlive()) {
          int fragments = commands
              .poll((buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length)), 10);
          assertTrue(System.nanoTime() - deadline < 0, "Benchmark kept retrying an unanswered request");
          idle.idle(fragments);
        }
        String output = Files.readString(log);
        assertTrue(benchmark.exitValue() != 0, output);
        assertEquals(1, received.size(), "Only one attempt of the first sample may be sent");
        assertEquals(new CancelOrder(1L), received.getFirst().command());
        assertTrue(output.contains("No reply after 1 attempts"), output);
        assertTrue(output.contains(received.getFirst().requestId().toString()), output);
        assertFalse(output.contains("p50:"),
            "A failed run must not publish statistics over partial samples\n" + output);
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        stopIfAlive(benchmark);
      }
    }
  }

  @Test
  void reportsNearestRankPercentilesAndConvertsNanosecondsToMicroseconds() {
    long[] samples = new long[100];
    for (int i = 0; i < samples.length; i++) {
      samples[i] = (100 - i) * 1_000L;
    }
    String report = AeronLatencyBenchmark.summarize(samples);
    assertEquals(50.0, metric(report, "p50"));
    assertEquals(99.0, metric(report, "p99"));
    assertEquals(100.0, metric(report, "max"));
  }

  @Test
  void usesNearestRankInsteadOfAveragingTheTwoMiddleSamples() {
    String report = AeronLatencyBenchmark.summarize(new long[]{4_000L, 1_000L, 3_000L, 2_000L});
    assertEquals(2.0, metric(report, "p50"));
    assertEquals(4.0, metric(report, "p99"));
    assertEquals(4.0, metric(report, "max"));
  }

  @Test
  void aSingleSampleDefinesEveryReportedPercentile() {
    String report = AeronLatencyBenchmark.summarize(new long[]{1_501L});
    assertEquals(1.501, metric(report, "p50"));
    assertEquals(1.501, metric(report, "p99"));
    assertEquals(1.501, metric(report, "max"));
  }

  @Test
  void cannotSummarizeAnEmptyRun() {
    assertThrows(IllegalArgumentException.class, () -> AeronLatencyBenchmark.summarize(new long[0]));
  }

  @ParameterizedTest
  @CsvSource({"-1, 1", "0, 0", "0, -1", "oops, 1"})
  void rejectsInvalidCountsBeforeConnecting(String warmup, String samples) {
    assertThrows(IllegalArgumentException.class, () -> AeronLatencyBenchmark.main(new String[]{warmup, samples}));
  }

  @Test
  void rejectsExtraArgumentsBeforeConnecting() {
    assertThrows(IllegalArgumentException.class, () -> AeronLatencyBenchmark.main(new String[]{"0", "1", "extra"}));
  }

  private static double metric(String report, String name) {
    Matcher matcher = Pattern.compile("(?m)^" + name + ": ([0-9]+\\.[0-9]{3}) us$").matcher(report);
    assertTrue(matcher.find(), "Missing microsecond metric " + name + "\n" + report);
    return Double.parseDouble(matcher.group(1));
  }

  private static Process startMain(Path tempDir, Path log, Class<?> mainClass, String... args) throws IOException {
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        mainClass.getName()));
    command.addAll(List.of(args));
    return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  private static void awaitOutput(Process process, Path log, String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.readString(log).contains(expected)) {
      assertTrue(process.isAlive(), "Process exited before startup\n" + Files.readString(log));
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for startup\n" + Files.readString(log));
      Thread.sleep(10);
    }
  }

  private static void stopIfAlive(Process process) throws InterruptedException {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      assertTrue(process.waitFor(3, TimeUnit.SECONDS), "Could not stop the test process");
    }
  }
}
