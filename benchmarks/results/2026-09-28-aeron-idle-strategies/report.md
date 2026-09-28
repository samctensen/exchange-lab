# Client idle-strategy comparison — 28 September 2026

## Conclusion

**Keep `sleep` as the benchmark default for this workload on this Mac.** Busy-spin
polling roughly doubled client CPU use without a consistent throughput benefit.
The millisecond latency bands persisted after removing explicit client parks.

At window eight, sleeping polling measured **1,131.959 requests/s**, **8.188 ms
p99**, and **50.631% client-thread CPU**. Busy-spin measured **1,105.108 requests/s**,
**9.834 ms p99**, and **99.295% client-thread CPU**. These are medians across five
runs per configuration; 100% CPU means one fully occupied core.

Across all four windows, switching to spin changed the ratio of throughput medians
by between **−2.37% and +1.68%**, while approximately doubling client CPU. Reported
p99 was essentially unchanged at window four and higher with spin at the other
windows. Five desktop runs do not establish statistical equivalence or a universal
policy, but they provide no compelling reason to pay the extra CPU cost here.

## Latency, throughput, and CPU

Each cell is the median of five per-run values. In particular, the p99 values are
medians of per-run percentiles, not pooled percentiles over all requests.

| Window | Client idle | Requests/s | p50 (ms) | p99 (ms) | Client thread CPU | Client JVM CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | sleep | 300.669 | 3.039 | 4.082 | 50.680% | 52.162% |
| 1 | spin | 301.807 | 3.030 | 4.905 | 99.180% | 102.718% |
| 2 | sleep | 295.693 | 6.949 | 8.095 | 50.683% | 52.080% |
| 2 | spin | 300.672 | 6.944 | 8.960 | 99.283% | 102.053% |
| 4 | sleep | 598.322 | 6.865 | 8.080 | 50.541% | 52.998% |
| 4 | spin | 602.456 | 6.912 | 8.077 | 99.395% | 103.106% |
| 8 | sleep | 1131.959 | 6.999 | 8.188 | 50.631% | 56.078% |
| 8 | spin | 1105.108 | 7.007 | 9.834 | 99.295% | 106.448% |

The CPU counters cover the measured phase only. Client-thread CPU includes the
thread running encoding, sending, polling, and reply handling. Client-JVM CPU also
includes its Aeron, JIT, GC, and other threads, so it can exceed 100%. The separate
engine/Media Driver/Archive JVM is **not included**. These figures cannot establish
the combined CPU budget of the whole system.

Sleeping polling is not a low-CPU configuration in this experiment. The loaded
Agrona `SleepingIdleStrategy` requests a **1,000 ns (1 µs) park** when no work is
done; its default constructor does not request a millisecond sleep. Actual park
duration depends on the OS. The [resolved bytecode](sleeping-idle-strategy.txt)
records the checked constructor and `idle(int)` implementation.

### CPU cost per completed request

For each run, client-JVM CPU microseconds per request were calculated as
`CPU percent × 10,000 / requests per second`, then the five values were summarized
by their median. This uses the rounded metrics in the raw reports.

| Window | Sleep client-JVM CPU (µs/request) | Spin client-JVM CPU (µs/request) |
| --- | ---: | ---: |
| 1 | 1735.7 | 3403.4 |
| 2 | 1766.0 | 3394.2 |
| 4 | 869.4 | 1725.4 |
| 8 | 491.4 | 963.6 |

The extra CPU was not offset by completing this workload substantially faster.

## Variation across runs

All 40 runs are included. These are observed ranges, not confidence intervals.

| Window | Idle | Requests/s range | Per-run p99 range (ms) |
| --- | --- | ---: | ---: |
| 1 | sleep | 260.935–304.517 | 4.061–4.111 |
| 1 | spin | 281.777–304.603 | 4.878–4.950 |
| 2 | sleep | 270.242–306.965 | 8.079–8.996 |
| 2 | spin | 262.784–309.270 | 8.047–12.942 |
| 4 | sleep | 556.531–611.862 | 8.028–11.943 |
| 4 | spin | 516.964–620.247 | 8.034–10.002 |
| 8 | sleep | 1112.525–1169.064 | 8.059–8.929 |
| 8 | spin | 1052.170–1201.373 | 8.048–12.140 |

Matching configurations within each round gives median spin/sleep throughput
ratios of **1.004, 1.003, 1.007, and 0.993** for windows 1, 2, 4, and 8. Spin had
higher throughput in three of five rounds at windows 1, 2, and 4, and two of five
rounds at window 8. This supports the same practical conclusion as the main table.

These sleeping baselines were rerun alongside spin; the earlier
[pipeline comparison](../2026-09-28-aeron-pipeline/report.md) came from a different
desktop time interval and build without CPU instrumentation. Differences between
those reports should not be attributed to the strategy switch.

## Method and environment

- Forty runs: windows 1, 2, 4, and 8 × `sleep` and `spin` × five rounds. A seeded
  shuffle varied the eight configurations' order in each round.
- Every run used fresh server and benchmark JVMs, a new Archive directory, an
  isolated driver directory, and a quiet server. Owned servers shut down before
  the next run; their temporary data was removed after shutdown.
- Each run sent 500 warmup requests and 2,000 measured requests. Warmup drained
  fully. There were 10,000 measured requests per strategy/window combination.
- Workload remained fresh-UUID `CancelOrder(1)` commands against an empty book,
  expecting `cancelled=false`, with no resend after a successful offer.
- The engine's log window stayed at eight. Its idle strategy remained busy-spin.
  Archive file and catalog sync levels remained **2**. Only the benchmark client's
  idle-strategy option varied within each window comparison.
- CPU-time counters were read just outside the measured phase's wall-clock timer,
  not inside the polling loop. Small boundary offsets and counter granularity can
  affect short phases. Unsupported counters would be reported as unavailable;
  both counters were available in all these runs.
- Hardware: Apple M4 Pro, 14 logical CPUs, 24 GiB RAM; macOS 26.6.2; Homebrew OpenJDK
  25.0.4.1. There was no CPU pinning or isolation from normal desktop activity.
  One-minute load average was about 3.52 before the experiment and 4.26 afterward.
- Runs completed between 16:07:59 and 16:12:14 America/Los_Angeles. There were no
  other exchange JVMs before or after the comparison, and all 40 runs succeeded.
- JVM-option environment variables were removed from child processes. Each JVM
  received `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED` explicitly.

The source was the uncommitted engine-batching and benchmark work on base commit
`1e0859acb3cd250c9c62ae0a04ae7af9151649e3`, captured in the implementation patch.
The isolated Maven build passed **481 tests and Spotless** before measurements
started. Production source hashes matched that build before the experiment and
remained unchanged afterward. Builds and tests did not run concurrently with the
experiment.

To repeat, run a quiet server with a new Archive directory for each case, then run
`AeronLatencyBenchmark.main` with arguments such as:

```text
500 2000 8 sleep
500 2000 8 spin
```

Repeat for windows 1, 2, and 4, varying run order. Report throughput, tail latency,
and CPU together.

Artifacts: [raw CSV](runs.csv), [raw JSON](runs.json), [aggregates and paired
ratios](summary.json), [environment and source hashes](environment.json),
[implementation patch](implementation.patch), and [raw logs](logs/).

## What this tells us to investigate next

Removing explicit client parks did not remove the roughly 3–8 ms latency bands.
That does not identify the remaining cause or prove client scheduling has no
effect. Archive forcing, Media Driver/Archive polling, and cross-thread scheduling
remain candidates. A busy-spin client also changes CPU contention, so this is not
a direct measurement of the duration of individual parks.

The next lesson should measure where the server waits: request admission to log
offer, log offer to recording confirmation, and confirmation to reply offer.
Stage timings or a focused recording of server thread/I/O activity can guide the
next change. Keep durability settings fixed while investigating.

This closed-loop cancellation benchmark excludes gRPC, the future Go backend, and
trade matching. It does not model a fixed external arrival rate or establish
production throughput. Five short runs and 500 warmup requests are also insufficient
to characterize long-term JIT, GC, thermal behavior, or rare latency spikes.
