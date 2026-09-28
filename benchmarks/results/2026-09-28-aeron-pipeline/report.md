# Aeron pipeline comparison — 28 September 2026

## Conclusion

With the current engine, increasing the benchmark client's window from one to
eight requests raised median throughput from **253.390 to 1,006.582 requests/s**
(**3.97×**). Median reported p50 latency increased from **3.990 to 7.992 ms**;
median reported p99 increased from **5.028 to 9.055 ms**.

Window eight had the highest throughput in every round. Window two gave little
throughput improvement over one while roughly doubling median latency. Window
four delivered about twice the throughput of one, with more variable tail latency.

Eight is the strongest throughput setting among the tested windows for this
workload. One provides the lowest individual-request latency. These results do
not establish a production optimum or the engine's maximum capacity.

## Results

Each column below is the median of five independent runs of that metric. The p99
column is the median of five per-run p99 values, not a pooled percentile over all
requests. Ratios use the unrounded throughput medians.

| Client window | Requests/s | Relative throughput | p50 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| 1 | 253.390 | 1.00× | 3.990 | 5.028 |
| 2 | 255.297 | 1.01× | 7.987 | 10.248 |
| 4 | 502.266 | 1.98× | 7.995 | 10.479 |
| 8 | 1006.582 | 3.97× | 7.992 | 9.055 |

All runs are included. Ranges describe the observed variation; they are not
confidence intervals.

| Client window | Requests/s range | Per-run p99 range (ms) | Largest observed latency (ms) |
| --- | ---: | ---: | ---: |
| 1 | 245.976–275.903 | 4.270–5.685 | 18.993 |
| 2 | 247.041–266.561 | 8.994–11.072 | 20.642 |
| 4 | 481.823–588.250 | 8.999–18.772 | 30.078 |
| 8 | 991.428–1175.871 | 8.042–10.338 | 15.079 |

## Method

- Twenty runs: five fresh server/benchmark JVM pairs per client window.
- Each run used 500 warmup requests, then 2,000 measured requests. Warmup drained
  completely before measurement. That is 10,000 measured requests per window.
- A seeded shuffle varied window order within each round; the exact sequence is
  saved in the environment metadata and raw results.
- Every run used a new temporary Archive directory and a separate driver
  directory. Quiet server mode disabled per-request console logging.
- Workload: `CancelOrder(1)` against an empty book, expecting `cancelled=false`.
  Every request used a fresh UUID and traversed the recording path. The benchmark
  did not resend successfully offered requests.
- The engine's log window stayed at eight in every run. The only experimental
  variable was the benchmark client's maximum number of outstanding requests.
- Archive file and catalog sync levels remained **2**. The engine used
  `BusySpinIdleStrategy`; the benchmark client used `SleepingIdleStrategy`.
- Throughput is completed measured requests divided by measured-phase elapsed
  time. Per-request latency includes encoding, unsuccessful offer retries, IPC,
  recording, processing, and reply handling. UUID/request construction is excluded
  from individual latency timers but included in throughput elapsed time.
- No failures occurred in the 20 runs. Each benchmark exited successfully, and
  each owned server shut down before the next run. No other exchange JVMs were
  running before or after the experiment. Temporary recordings were removed after
  shutdown; raw benchmark and server logs were retained.

## Environment and reproducibility

Apple M4 Pro, 14 logical CPUs, 24 GiB RAM, macOS 26.6.2, Homebrew OpenJDK 25.0.4.1.
This was a normal desktop session with no CPU pinning or isolation from other
applications. The one-minute load average was about 2.01 before the experiment
and 3.16 afterward. Runs completed between 15:54:55 and 15:57:19 America/Los_Angeles.

The build was the current uncommitted engine-batching and benchmark implementation
on base commit `1e0859acb3cd250c9c62ae0a04ae7af9151649e3`. Its full verification
had passed **475 tests and Spotless** before measurement. The benchmark ran from
that isolated Maven build, outside the editor-watched checkout. Production source
hashes matched the build before the experiment and remained unchanged afterward.

`JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, and `_JAVA_OPTIONS` were removed from the
child environment for consistent JVM options. Both JVMs received
`--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED` explicitly. Each pair used a
matching, unique `-Djava.io.tmpdir` so it could not share a running demo's driver.

To repeat, use the documented server/benchmark launch flow with a new Archive
directory per run. Run the server with `--quiet`, then supply these benchmark
arguments, repeating and varying the order:

```text
500 2000 1
500 2000 2
500 2000 4
500 2000 8
```

Artifacts:

- [Raw per-run CSV](runs.csv) and [JSON](runs.json)
- [Aggregated metrics](summary.json)
- [Environment, source hashes, JVM settings, and run order](environment.json)
- [Production implementation patch against the base commit](implementation.patch)
- [Raw logs](logs/)

## Interpretation and next lesson

The results show that more outstanding requests can raise durable-request
throughput on this implementation while increasing the time an individual request
spends waiting. They do not attribute the gain solely to the new engine batching:
the previous serial engine was not measured here.

This is a closed-loop cancellation workload. Completion makes room for another
request, so a slowdown also reduces the arrival rate. It does not measure an
independent external traffic rate, matching trades, gRPC, or a Go backend. With
2,000 samples per run, p99 reflects only about the slowest 20 observations; the
observed spikes should motivate longer runs before setting latency targets.

The roughly 4–8 ms latency bands make client polling and thread wake-up delays a
useful next variable to investigate. That is a hypothesis, not a profile result:
these measurements do not separate client sleeping, Archive forcing, and server
scheduling. The next experiment should compare sleeping and busy-spin clients at
the same windows, measuring CPU use as well as latency and throughput, while
keeping Archive durability settings unchanged.
