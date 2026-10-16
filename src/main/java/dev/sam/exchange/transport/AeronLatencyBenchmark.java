package dev.sam.exchange.transport;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.BitUtil;
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
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;

public class AeronLatencyBenchmark {
  private static final CancelResult EXPECTED_RESULT = new CancelResult(1L, false);

  record Measurement(long[] latencies, long elapsedNanos, long threadCpuNanos, long processCpuNanos) {
  }

  private record PendingSample(int index, UUID requestId, long startedNanos, long deadlineNanos) {
  }

  public static void main(String[] args) {
    if (Arrays.asList(args).contains("--help")) {
      if (args.length != 1)
        throw new IllegalArgumentException("--help must be used alone");
      System.out.println("""
          Usage: AeronLatencyBenchmark [warmupCount] [sampleCount] [maxInFlight] [sleep|spin] [options]
          Start a separate AeronEngineServer with a fresh, empty archive and --quiet before benchmarking.

            warmupCount          Completed requests before measurement, 0..1000000 (default: 500)
            sampleCount          Measured requests, 1..1000000 (default: 2000)
            maxInFlight          Maximum outstanding requests, positive integer (default: 1)
            sleep|spin           Client polling idle strategy (default: sleep)
            --archive-counters   Report Archive write counters (default: disabled)
            --aeron-directory=p  Existing engine Media Driver directory
                                  (default: java.io.tmpdir/exchange-lab-aeron)
            --help               Print this help and exit; use by itself

          Named options may appear before, between, or after positional arguments.
          Each send and reply wait has a 5-second timeout, with one attempt per request.
          Failed runs do not print performance statistics.
          """);
      return;
    }
    BenchmarkConfig config = BenchmarkConfig.parse(args);
    int warmupCount = config.warmupCount();
    int sampleCount = config.sampleCount();
    int maxInFlight = config.maxInFlight();
    String idleMode = config.idleMode();
    IdleStrategy idle = idleStrategy(idleMode);

    // Use a separate server with a fresh archive directory and --quiet for a comparable baseline.
    Measurement measured;
    String archiveReport = "";
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectory().toString()));
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication publication = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(),
            1)) {
      // A timeout fails this run; automatic resends would change the workload being measured.
      AeronRequestClient client = new AeronRequestClient(publication, replies,
          new ClientConfig(Duration.ofSeconds(5), 1));
      ArchiveWriteCounters counters = config.archiveCounters()
          ? ArchiveWriteCounters.find(aeron.countersReader())
          : null;
      // This workload uses one fixed-size, unfragmented cancel frame per fresh request.
      int encodedLength = new SbeRequestCodec().encode(new CommandRequest(new UUID(0, 0), new CancelOrder(1)),
          new ExpandableArrayBuffer(64), 0);
      long recordedBytesPerRequest = BitUtil.align(encodedLength + DataHeaderFlyweight.HEADER_LENGTH,
          FrameDescriptor.FRAME_ALIGNMENT);
      if (counters != null) {
        if (encodedLength > publication.maxPayloadLength())
          throw new IllegalStateException("Archive counter diagnostics require unfragmented benchmark requests");
        counters.awaitSnapshot(0);
      }

      // Drain all warmup replies before starting the measured phase.
      measure(client, warmupCount, maxInFlight, SystemNanoClock.INSTANCE, idle);
      ArchiveWriteCounters.Snapshot before = counters == null
          ? null
          : counters.awaitSnapshot(warmupCount * recordedBytesPerRequest);
      measured = measure(client, sampleCount, maxInFlight, SystemNanoClock.INSTANCE, idle);
      if (counters != null) {
        var after = counters.awaitSnapshot(((long) warmupCount + sampleCount) * recordedBytesPerRequest);
        archiveReport = counters.summarize(before, after, measured.elapsedNanos());
      }
    }

    // Sorting and console output happen after every measured request has completed successfully.
    System.out.println("Warmup: " + warmupCount + " requests");
    System.out.println("Attempts per request: 1");
    System.out.println("Max in flight: " + maxInFlight);
    System.out.println("Client idle strategy: " + idleMode);
    System.out.print(summarize(measured));
    System.out.print(archiveReport);
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
    if (count < 0 || count > BenchmarkConfig.MAX_COUNT || maxInFlight < 1) {
      throw new IllegalArgumentException(
          "Sample count must be between 0 and " + BenchmarkConfig.MAX_COUNT + "; max in flight must be positive");
    }
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
    if (sorted[0] < 0) {
      throw new IllegalArgumentException("Latency samples must not be negative");
    }
    // Nearest-rank percentiles: the first rank covering at least 50% or 99% of the samples.
    int p50Index = (int) ((sorted.length * 50L + 99) / 100) - 1;
    int p99Index = (int) ((sorted.length * 99L + 99) / 100) - 1;
    return String.format(Locale.ROOT,
        "Samples: %d requests%nRound-trip latency (microseconds):%np50: %.3f us%np99: %.3f us%nmax: %.3f us%n",
        sorted.length, sorted[p50Index] / 1_000.0, sorted[p99Index] / 1_000.0, sorted[sorted.length - 1] / 1_000.0);
  }
}
