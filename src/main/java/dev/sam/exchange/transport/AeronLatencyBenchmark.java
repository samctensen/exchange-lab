package dev.sam.exchange.transport;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

public class AeronLatencyBenchmark {
  private static final int DEFAULT_WARMUP_COUNT = 500;
  private static final int DEFAULT_SAMPLE_COUNT = 2_000;
  private static final CancelResult EXPECTED_RESULT = new CancelResult(1L, false);

  record Measurement(long[] latencies, long elapsedNanos, long threadCpuNanos, long processCpuNanos) {
  }

  private record PendingSample(int index, UUID requestId, long startedNanos, long deadlineNanos) {
  }

  public static void main(String[] args) {
    if (args.length > 4) {
      throw new IllegalArgumentException(
          "Usage: AeronLatencyBenchmark [warmupCount] [sampleCount] [maxInFlight] [sleep|spin]");
    }
    int warmupCount = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_WARMUP_COUNT;
    int sampleCount = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_SAMPLE_COUNT;
    int maxInFlight = args.length > 2 ? Integer.parseInt(args[2]) : 1;
    if (warmupCount < 0 || sampleCount < 1 || maxInFlight < 1) {
      throw new IllegalArgumentException(
          "Warmup count must be non-negative; sample count and max in flight must be positive");
    }
    String idleMode = args.length > 3 ? args[3] : "sleep";
    IdleStrategy idle = idleStrategy(idleMode);

    // Use a separate server with a fresh archive directory and --quiet for a comparable baseline.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();
    Measurement measured;
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {
      // A timeout fails this run; automatic resends would change the workload being measured.
      AeronRequestClient client = new AeronRequestClient(publication, replies,
          new ClientConfig(Duration.ofSeconds(5), 1));

      // Drain all warmup replies before starting the measured phase.
      measure(client, warmupCount, maxInFlight, SystemNanoClock.INSTANCE, idle);
      measured = measure(client, sampleCount, maxInFlight, SystemNanoClock.INSTANCE, idle);
    }

    // Sorting and console output happen after every measured request has completed successfully.
    System.out.println("Warmup: " + warmupCount + " requests");
    System.out.println("Attempts per request: 1");
    System.out.println("Max in flight: " + maxInFlight);
    System.out.println("Client idle strategy: " + idleMode);
    System.out.print(summarize(measured));
  }

  static IdleStrategy idleStrategy(String mode) {
    return switch (mode) {
      case "sleep" -> new SleepingIdleStrategy();
      case "spin" -> new BusySpinIdleStrategy();
      default -> throw new IllegalArgumentException("Client idle strategy must be sleep or spin: " + mode);
    };
  }

  static Measurement measure(AeronRequestClient client, int count, int maxInFlight, NanoClock clock,
      IdleStrategy idle) {
    long[] latencies = new long[count];
    Map<UUID, PendingSample> inFlight = new LinkedHashMap<>();
    SbeRequestCodec codec = new SbeRequestCodec();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    long timeoutNanos = client.config().timeout().toNanos();
    PendingSample pendingOffer = null;
    int encodedLength = 0;
    int sent = 0;
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    var os = ManagementFactory.getOperatingSystemMXBean();
    // Sample CPU counters only at phase boundaries, outside the latency/throughput timers.
    long processCpuStarted = os instanceof com.sun.management.OperatingSystemMXBean extended
        ? extended.getProcessCpuTime()
        : -1;
    long threadCpuStarted = threads.isCurrentThreadCpuTimeSupported() && threads.isThreadCpuTimeEnabled()
        ? threads.getCurrentThreadCpuTime()
        : -1;
    long runStarted = clock.nanoTime();

    while (sent < count || !inFlight.isEmpty()) {
      int work = 0;
      while (sent < count && inFlight.size() < maxInFlight) {
        if (pendingOffer == null) {
          // Fresh UUIDs exercise recording rather than the response cache. Construction is outside the latency timer.
          CommandRequest request = new CommandRequest(UUID.randomUUID(), new CancelOrder(1L));
          long started = clock.nanoTime();
          pendingOffer = new PendingSample(sent, request.requestId(), started, started + timeoutNanos);
          encodedLength = codec.encode(request, buffer, 0);
        }

        long result = client.trySend(buffer, 0, encodedLength);
        if (result < 0) {
          if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException(
                "Request publication failed for " + pendingOffer.requestId() + "; offer result: " + result);
          }
          if (clock.nanoTime() - pendingOffer.deadlineNanos() >= 0) {
            throw new IllegalStateException(
                "Timed out sending request " + pendingOffer.requestId() + "; last offer result: " + result);
          }
          // Keep these bytes and their original timer, but still poll replies while back pressured.
          break;
        }

        inFlight.put(pendingOffer.requestId(), new PendingSample(pendingOffer.index(), pendingOffer.requestId(),
            pendingOffer.startedNanos(), clock.nanoTime() + timeoutNanos));
        pendingOffer = null;
        sent++;
        work++;
      }

      work += client.pollResponses(response -> {
        PendingSample sample = inFlight.remove(response.requestId());
        // Streams can carry unrelated or duplicate replies; only an active UUID can complete a sample.
        if (sample == null)
          return;
        long elapsed = clock.nanoTime() - sample.startedNanos();
        if (!EXPECTED_RESULT.equals(response.result())) {
          throw new IllegalStateException("Unexpected benchmark response for request " + response.requestId() + ": "
              + response.result() + "; use a fresh, empty benchmark archive directory");
        }
        latencies[sample.index()] = elapsed;
      }, Math.min(maxInFlight, 10));

      long now = clock.nanoTime();
      for (PendingSample sample : inFlight.values()) {
        if (now - sample.deadlineNanos() >= 0) {
          throw new IllegalStateException(
              "No reply after 1 attempts for request " + sample.requestId() + "; outcome unknown");
        }
      }
      idle.idle(work);
    }
    long elapsed = clock.nanoTime() - runStarted;
    long threadCpuEnded = threadCpuStarted >= 0 ? threads.getCurrentThreadCpuTime() : -1;
    long processCpuEnded = os instanceof com.sun.management.OperatingSystemMXBean extended
        ? extended.getProcessCpuTime()
        : -1;
    return new Measurement(latencies, elapsed, cpuDelta(threadCpuStarted, threadCpuEnded),
        cpuDelta(processCpuStarted, processCpuEnded));
  }

  private static long cpuDelta(long started, long ended) {
    return started < 0 || ended < started ? -1 : ended - started;
  }

  static String summarize(Measurement measured) {
    return summarize(measured.latencies(), measured.elapsedNanos())
        + cpuUsage("Client thread CPU", measured.threadCpuNanos(), measured.elapsedNanos())
        + cpuUsage("Client JVM CPU", measured.processCpuNanos(), measured.elapsedNanos());
  }

  private static String cpuUsage(String label, long cpuNanos, long elapsedNanos) {
    // 100% is one fully occupied core. The JVM total includes other threads and can exceed 100%.
    return cpuNanos < 0
        ? label + ": unavailable" + System.lineSeparator()
        : String.format(Locale.ROOT, "%s: %.3f%%%n", label, 100.0 * cpuNanos / elapsedNanos);
  }

  static String summarize(long[] latencies, long elapsedNanos) {
    if (elapsedNanos <= 0)
      throw new IllegalArgumentException("Measured duration must be positive");
    // Latencies overlap when pipelining. Throughput uses elapsed time for the entire measured phase.
    return summarize(latencies) + String.format(Locale.ROOT, "Throughput: %.3f requests/s%n",
        latencies.length * 1_000_000_000.0 / elapsedNanos);
  }

  static String summarize(long[] latencies) {
    if (latencies.length == 0) {
      throw new IllegalArgumentException("At least one latency sample is required");
    }
    long[] sorted = latencies.clone();
    Arrays.sort(sorted);
    // Nearest-rank percentiles: the first rank covering at least 50% or 99% of the samples.
    int p50Index = (int) ((sorted.length * 50L + 99) / 100) - 1;
    int p99Index = (int) ((sorted.length * 99L + 99) / 100) - 1;
    return String.format(Locale.ROOT,
        "Samples: %d requests%nRound-trip latency (microseconds):%np50: %.3f us%np99: %.3f us%nmax: %.3f us%n",
        sorted.length, sorted[p50Index] / 1_000.0, sorted[p99Index] / 1_000.0, sorted[sorted.length - 1] / 1_000.0);
  }
}
