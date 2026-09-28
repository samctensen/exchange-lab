package dev.sam.exchange.transport;

import java.util.Arrays;
import java.util.Locale;

/** Bounded diagnostic samples. Written by the engine agent; summarize only after it stops. */
final class EngineStageTimings {
  private static final String[] STAGES = {"Log offer", "Recording observation", "Process and encode", "Reply offer",
      "Server total"};
  private final int warmupCount;
  private final int sampleLimit;
  private final long[][] samples;
  private int skipped;
  private int count;

  EngineStageTimings(int warmupCount, int sampleLimit) {
    if (warmupCount < 0 || sampleLimit < 1 || sampleLimit > 1_000_000) {
      throw new IllegalArgumentException("Stage timing requires warmup >= 0 and samples between 1 and 1000000");
    }
    this.warmupCount = warmupCount;
    this.sampleLimit = sampleLimit;
    samples = new long[STAGES.length][sampleLimit];
  }

  // Only fresh logged requests with successfully published replies reach this method.
  void record(long admitted, long offered, long recorded, long prepared, long replied) {
    if (skipped < warmupCount) {
      skipped++;
      return;
    }
    if (count == sampleLimit) {
      return;
    }
    samples[0][count] = offered - admitted;
    samples[1][count] = recorded - offered;
    samples[2][count] = prepared - recorded;
    samples[3][count] = replied - prepared;
    samples[4][count] = replied - admitted;
    count++;
  }

  String summarize() {
    StringBuilder report = new StringBuilder("Stage timing: " + count + "/" + sampleLimit + " samples, skipped "
        + skipped + "/" + warmupCount + " completed logged requests\n");
    for (int stage = 0; stage < STAGES.length && count > 0; stage++) {
      long[] sorted = Arrays.copyOf(samples[stage], count);
      Arrays.sort(sorted);
      double sum = 0;
      for (long sample : sorted) {
        sum += sample;
      }
      report.append(String.format(Locale.ROOT, "%s: mean=%.3f p50=%.3f p99=%.3f max=%.3f us%n", STAGES[stage],
          sum / count / 1_000, sorted[(int) Math.ceil(count * 0.50) - 1] / 1_000.0,
          sorted[(int) Math.ceil(count * 0.99) - 1] / 1_000.0, sorted[count - 1] / 1_000.0));
    }
    return report.toString();
  }
}
