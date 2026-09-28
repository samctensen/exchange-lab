# Server stage timing — 28 September 2026

## Conclusion

**Waiting to observe the recorded position dominates the measured server path.** Across five instrumented runs at each window, the median share of mean server time was **99.84% at window 1** and **99.97% at window 8**. Server p50 was about **4 ms** and **8 ms**, respectively. Log offering, the missing-order cancellation path plus encoding, and reply offering were each measured in microseconds.

This localizes the delay to **log offer → recorded-position observation**. It does **not** identify how much is Archive polling, writing/forcing data, OS scheduling, waiting behind earlier requests, or the engine noticing progress. It is not a measurement of pure disk-sync latency.

## Stage results

Each cell is the median of five run-level statistics, in microseconds. These are separate distributions; do not add stage percentiles to reconstruct the total percentile.

| Stage | Window 1 p50 | Window 1 p99 | Window 8 p50 | Window 8 p99 |
| --- | ---: | ---: | ---: | ---: |
| Log offer | 1.292 | 19.291 | 0.542 | 10.083 |
| Recording observation | 3,981.042 | 4,226.750 | 7,979.875 | 8,293.625 |
| Process and encode | 0.542 | 10.167 | 0.166 | 3.583 |
| Reply offer | 0.375 | 9.667 | 0.167 | 3.958 |
| Server total | 3,986.375 | 4,231.167 | 7,981.417 | 8,294.500 |

For each run, the four stage means sum to that run's mean server total, apart from rounding. The recording share above is computed per run from those means, then summarized with the median. Independently computed medians of means need not add up.

### What each interval includes

- **Log offer:** admission of a complete decoded request by the engine agent to successful request-log publication, including back pressure.
- **Recording observation:** successful log offer to observing that request's recorded end position while it is the FIFO head. Time behind earlier work and reply back pressure is included here for queued entries.
- **Process and encode:** recorded-position observation through state-machine processing and response encoding.
- **Reply offer:** encoded response ready through successful reply publication, including offer retries.
- **Server total:** admission through successful reply publication for that same request.

Inbound IPC/decoding before admission and client polling after publication are outside the server total. Client/server percentiles cannot be subtracted to isolate transport latency. Replies accepted by the publication count as server completion; this is not an acknowledgement that the client has consumed them.

## Instrumentation control

The same build ran with timing disabled and enabled. Table entries are median run-level values, with the full five-run throughput range. Client polling remained `sleep` throughout.

| Client window | Timing | Requests/s | Requests/s range | Client p50 (µs) | Client p99 (µs) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | off | 251.894 | 250.850–266.354 | 3,995.459 | 4,179.041 |
| 1 | on | 252.340 | 250.347–259.812 | 3,991.708 | 4,236.375 |
| 8 | off | 1,016.914 | 1,002.092–1,036.934 | 7,977.708 | 9,040.666 |
| 8 | on | 1,015.889 | 1,004.101–1,030.022 | 7,989.542 | 8,298.625 |

Timing-on median throughput differed from timing-off by **+0.18% at window 1** and **-0.10% at window 8**. No clear throughput penalty is visible at this scale. Five desktop runs do not establish zero instrumentation overhead or a statistically significant tail-latency change. This compares the runtime flag in the same build; it does not isolate the cost of the added timestamp fields themselves.

## Method and validation

- Ran 16:26:37–16:28:49 PDT on 28 September 2026.
- Apple M4 Pro, 14 logical CPUs, 24 GiB RAM; macOS 26.6.2; Homebrew OpenJDK 25.0.4.1.
- Five rounds, each containing windows 1/8 × timing off/on, shuffled with seed `20260928`: **20 runs** total.
- Every run used fresh server/client JVMs, an empty Archive directory, and a unique temporary Aeron directory.
- Each run sent 500 warmup and 2,000 measured fresh-UUID `CancelOrder(1)` requests, expecting `cancelled=false`. Warmup drained before client measurement. Timing-on servers skipped exactly 500 completed logged requests and collected exactly 2,000 samples; all ten reports confirmed those counts.
- Quiet server; engine log window **8**; engine `BusySpinIdleStrategy`; client `SleepingIdleStrategy`; Archive file/catalog sync levels **2/2**. Durability behavior unchanged.
- Timing storage is bounded and preallocated. Existing deadline clock reads provide admission/offer/prepared timestamps; timing adds clock reads at recorded-position observation and successful reply offer. It prints only after the agent stops. Cached retries and recovery are excluded.
- This is a closed-loop workload at fixed concurrency, not an externally paced arrival-rate test. It exercises missing-order cancellation, not trading across a populated order book.
- No CPU pinning or desktop isolation. No other exchange JVM was present before the run; none remained afterward. Tests/builds were not run alongside measurements.
- **498 tests passed**, zero failures/errors/skips, with Spotless verification. Coverage includes per-entry timestamps, log back pressure, recording/FIFO waits, blocked replies, cached retries, warmup/cap handling, flag validation, and process shutdown reporting.
- Main-source hashes matched the verified build and remained unchanged throughout the experiment. Measurements from earlier experiments were not used as the control.

## Reproduce

Use the source snapshot described in `environment.json` (base commit plus `implementation.patch`), build with Java 25, and configure the usual JVM argument `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED`.

Server program arguments for an instrumented run:

```text
<new-archive-directory> --quiet --stage-timing=500,2000
```

Benchmark program arguments:

```text
500 2000 1 sleep
```

Repeat with window `8`. Stop the server with **⌃C** after each benchmark to print stage statistics. Repeat both windows without `--stage-timing` for controls, using new directories/JVMs every time. Timing is disabled by default. Run only one benchmark client against each diagnostic server, since its warmup/sample counts span all fresh logged completions.

## Next lesson

Read Archive's **total write time, maximum write time, and bytes written** counters while keeping sync level 2. The locally installed Aeron **1.53.0** source shows `RecordingWriter.onBlock` timing the write path, including `FileChannel.force` when configured, and `ArchiveConductor.Recorder` publishing those counters. These are aggregate recording-write metrics, not per-request latency distributions; batched requests can share a write, so do not divide them into request timings without accounting for batching.

Start with window 1 and take counter deltas over a defined interval. That will help test whether writing/forcing accounts for the long recording-observation interval. A recorder-idle-strategy comparison can then test the polling contribution while preserving the durability contract.

## Saved evidence

- [Per-run CSV](runs.csv) and [JSON](runs.json)
- [Summary statistics](summary.json)
- [Environment, run order, hashes, and settings](environment.json)
- [Source changes against the recorded base](implementation.patch)
- [Raw benchmark and server logs](logs/)
