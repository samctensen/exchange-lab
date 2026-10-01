# Engine log-window comparison — 1 October 2026

## Conclusion

**An engine/client window of 32 was the strongest tested setting for this local cancellation workload.** It completed roughly four times as many requests per second as 8/8, with nearly the same median latency and similar per-run p99s. With client concurrency fixed at 32, increasing the engine window from 8 to 32 improved both throughput and client latency. Merely increasing client concurrency while leaving the engine at 8 mostly added queueing.

Archive file and catalog sync levels remained **2/2** throughout. The engine still executes only the recorded FIFO head, with one order-book writer. The default engine window and gRPC gateway concurrency remain **8**. These direct-IPC results justify testing a larger window through the gateway before choosing an application default.

## Matched client and engine windows

Each cell is the median of five per-run statistics. The p99 range shows all five per-run p99s; samples were not pooled.

| Engine window | Client window | Requests/s | Client p50 (ms) | Client p99 (ms) | Per-run p99 range (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| 8 | 8 | 1,011.3 | 7.987 | 8.905 | 8.152–9.020 |
| 16 | 16 | 2,039.3 | 7.978 | 9.044 | 8.946–14.939 |
| 32 | 32 | 4,077.1 | 7.980 | 8.987 | 8.199–9.014 |

Within each round, 32/32 delivered 3.93–4.06 times the throughput of 8/8 (median ratio 4.01). Median latency was essentially unchanged. The 16/16 runs had more variable tails, including per-run p99s around 13 and 15 ms. Five short runs cannot establish a rare-event latency guarantee or prove that 32 will always have better tails than 16.

These pairs change both engine capacity and offered concurrency. The following control changes only the engine setting.

## Fixed client window of 32

| Engine window | Client window | Requests/s | Client p50 (ms) | Client p99 (ms) | Per-run p99 range (ms) |
| --- | --- | ---: | ---: | ---: | ---: |
| 8 | 32 | 1,032.2 | 31.944 | 33.040 | 31.927–38.010 |
| 16 | 32 | 2,041.7 | 15.974 | 17.036 | 16.091–23.004 |
| 32 | 32 | 4,077.1 | 7.980 | 8.987 | 8.199–9.014 |

The 32/32 row is the same five runs shown above. Against 8/32, the median within-round throughput ratio for 32/32 was 3.91; its median-latency ratio was 0.25 and p99 ratio 0.27. The engine limit was constraining throughput in this workload, rather than simply relocating waiting without improving completion time.

With engine 8, client window 32 delivers similar throughput to client window 8 while retaining roughly four times as many requests in flight. That extra concurrency mostly waits. This illustrates why increasing a queue or client window by itself can hurt latency.

Server timing begins after a complete request is admitted. Inbound IPC waiting is included in client latency but excluded from server stages. Mean server time stayed around 7.7–7.9 ms in all five configurations, even when client latency was much higher. Client and server percentiles must not be subtracted to derive an inbound-wait percentile.

## Archive write-time evidence

These are medians across the five runs with matching windows:

| Engine/client window | Aggregate write time / client elapsed | Timed write µs per completed request | Mean server time (ms) |
| --- | ---: | ---: | ---: |
| 8/8 | 99.910% | 987.676 | 7.874 |
| 16/16 | 99.814% | 489.441 | 7.798 |
| 32/32 | 99.703% | 244.582 | 7.738 |

The amortized column is measured Archive write nanoseconds divided by 2,000 completed requests. It is neither an individual request's write latency nor the mean duration of a physical write. The recorder counters do not provide a write-operation count. Their timed path includes the channel write and configured force operation; see the [pinned counter semantics](../2026-10-01-aeron-archive-counters/counter-semantics.json).

**Inference:** permitting more requests to reach Archive together amortizes the timed write/force cost across more requests. The approximately halved cost per request at each doubling supports that explanation, while request latency remains near the recording-observation interval. The counters do not isolate write from force, establish exact batch sizes, or measure physical device latency directly.

Recording observation accounted for over 99.95% of mean server time in the matched-window runs (median shares per configuration). Client polling-thread CPU stayed near 51–52% of one core. Median client-JVM CPU rose from 54.7% at 8/8 to 70.5% at 32/32; the separate server JVM is excluded from those CPU measurements.

## Implementation

- `AeronEngineServer` accepts `--log-window=<positive integer>`, rejects invalid/duplicate values before starting resources, and prints the selected value at startup.
- `AeronEngineAgent` stores the validated bound per instance. Existing constructors retain the default of eight; a new overload accepts an explicit bound.
- Both log-admission capacity checks use that bound. FIFO execution, recorded-position checks, request deadlines, deduplication, and unsent-reply back pressure retain their existing behavior.
- The bound covers offered requests awaiting execution. Client outstanding requests, inbound IPC data, one pending admission, and the pending reply are separate parts of the request path. It is not a global memory or ingress limit.
- The configured window also changes how much admission work one duty cycle can do and the size of the existing linear queued-UUID scan. Results therefore measure the complete configuration change, not a pure Archive-only batching primitive.

## Method and validation

- 13:24:13–13:25:05 PDT, 1 October 2026.
- Apple M4 Pro, 14 logical CPUs, 24 GiB memory; macOS 26.6.2; Homebrew OpenJDK 25.0.4.1; Aeron 1.53.0 and Agrona 2.6.0.
- Five shuffled rounds (seed `20261001`) of engine/client pairs 8/8, 16/16, 32/32, 8/32, and 16/32: **25 runs**.
- Fresh server/client JVMs, an empty Archive, and a unique temporary Aeron directory for every run. Quiet server, engine busy-spin polling, client sleeping polling, stage timing enabled, and Archive counters enabled throughout.
- 500 warmup requests followed by 2,000 measured requests per run. Missing-order `CancelOrder(1)`, fresh UUIDs, one successful send per request, expected `cancelled=false` results. Every run collected exactly 2,000 stage samples after 500 skipped completions and measured 128,000 recorded bytes.
- Counter baselines/final snapshots are collected outside the measured client timers. The byte/tuple settling check reduces separately published counter boundary races; it is not an atomic snapshot. Maximum write counters in the raw data are lifetime maxima, including warmup, and are not subtracted.
- **522 tests passed**, zero failures/errors/skips, plus Spotless. New coverage checks default and explicit bounds 1/8/16/32, FIFO execution and slot refill, invalid constructor/CLI values, duplicate CLI flags, and real server/Archive runs at 16 and 32. The integration tests were first observed failing against the old server.
- Full verification ran in an isolated source copy before measurements. Source hashes matched that build and remained unchanged during the experiment. No other exchange JVMs were running before measurement; none remained afterward. Builds/tests did not run concurrently with measurements.
- No CPU affinity or desktop isolation. This short closed-loop benchmark measures performance at a selected concurrency, not at a fixed external arrival rate. It excludes gRPC, the future Go backend, populated-book matching, sustained overload, and production tail guarantees. All configurations used diagnostics; no timing-off control was repeated here.

## Reproduce

Use Java 25 and JVM option `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED`. Both processes must share `java.io.tmpdir`.

Server program arguments, with a new Archive directory each time:

```text
<new-archive-directory> --quiet --stage-timing=500,2000 --log-window=32
```

Benchmark program arguments:

```text
500 2000 32 sleep --archive-counters
```

Stop the server with **⌃C** after the benchmark to print its stage report. Repeat with the engine/client pairs above. The captured [runner](runner.py) records the exact experiment; its repository, verified-build, and output paths are local to this run. Use a fresh output directory and a build/classpath matching the sources when adapting it.

## Next learning step

Measure the full gRPC gateway path with bounded gateway and engine concurrency at 8, 16, and 32, including latency, rejection/timeout rates, and queue pressure. That establishes whether the IPC gain survives protobuf mapping, gateway scheduling, and realistic admission limits. Set a latency target before selecting the default; keep the queue bounded and make overload behavior explicit.

## Saved evidence

- [Raw CSV](runs.csv), [JSON](runs.json), and [all benchmark/server logs](logs/)
- [Summary statistics, full ranges, and within-round ratios](summary.json)
- [Environment, run order, settings, validation, and source hashes](environment.json)
- [Implementation patch against the recorded base commit](implementation.patch)
- [Exact experiment runner](runner.py)
