package dev.sam.exchange.transport;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

public class AeronLatencyBenchmark {
  private static final int DEFAULT_WARMUP_COUNT = 500;
  private static final int DEFAULT_SAMPLE_COUNT = 2_000;
  private static final CancelResult EXPECTED_RESULT = new CancelResult(1L, false);

  public static void main(String[] args) {
    if (args.length > 2) {
      throw new IllegalArgumentException("Usage: AeronLatencyBenchmark [warmupCount] [sampleCount]");
    }
    int warmupCount = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_WARMUP_COUNT;
    int sampleCount = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_SAMPLE_COUNT;
    if (warmupCount < 0 || sampleCount < 1) {
      throw new IllegalArgumentException("Warmup count must be non-negative and sample count must be positive");
    }

    // Use a separate server with a fresh journal and --quiet for a comparable baseline.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();
    long[] latencies = new long[sampleCount];
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {
      // A timeout fails this run; automatic resends would change the workload being measured.
      AeronRequestClient client = new AeronRequestClient(publication, replies,
          new ClientConfig(Duration.ofSeconds(5), 1));

      for (int i = 0; i < warmupCount; i++) {
        sendTimedRequest(client);
      }
      for (int i = 0; i < sampleCount; i++) {
        latencies[i] = sendTimedRequest(client);
      }
    }

    // Sorting and console output happen after every measured request has completed successfully.
    System.out.println("Warmup: " + warmupCount + " requests");
    System.out.println("Attempts per request: 1");
    System.out.print(summarize(latencies));
  }

  private static long sendTimedRequest(AeronRequestClient client) {
    // A new UUID exercises the journal and engine instead of the server's response cache.
    // Request construction is outside the timer; the timer covers send() through its matching reply.
    CommandRequest request = new CommandRequest(UUID.randomUUID(), new CancelOrder(1L));
    long started = System.nanoTime();
    CommandResult result = client.send(request);
    long elapsed = System.nanoTime() - started;

    if (!EXPECTED_RESULT.equals(result)) {
      throw new IllegalStateException("Unexpected benchmark response for request " + request.requestId() + ": " + result
          + "; use a fresh, empty benchmark journal");
    }
    return elapsed;
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
