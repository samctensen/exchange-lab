# Archive write-time counters — 1 October 2026

## Conclusion

**Archive's timed write path occupies almost the entire measured phase in this workload.** The median counter delta was **99.20% of client elapsed time at window 1** and **99.91% at window 8**. All ten runs used file/catalog sync levels **2/2**, and every measured interval recorded exactly **128,000 bytes** for 2,000 fresh cancel requests.

The earlier stage timing localized the delay to log offer → recorded-position observation. These counters narrow the dominant work to Archive's timed block-write path, which includes buffer preparation/checksum handling, channel writes, and the configured `FileChannel.force`. The interval also includes any thread scheduling delays that occur inside it. This does not separate `write` from `force` or measure physical storage latency directly.

**Next focus: durable-write batching and storage behavior.** The immediate next experiment can make the bounded engine log window configurable and compare 8, 16, and 32 under the same sync policy, reporting throughput and tail latency. A larger window may let more requests share each forced write; that remains a hypothesis to test. Keep the default unchanged until the measurements justify a choice.

## Results

Each entry is the median of five run-level measurements. Client polling is `sleep`; both server stage timing and Archive counter diagnostics are enabled.

| Client window | Requests/s | Client p50 (ms) | Client p99 (ms) | Write time / client elapsed | Amortized write time/request (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 261.422 | 3.970 | 5.059 | 99.204% | 3.797 |
| 8 | 1,015.408 | 7.981 | 9.022 | 99.905% | 0.984 |

Amortized write time divides aggregate recorder write time by completed requests. It is not an individual request's latency or an average duration per write: multiple requests may share a write and overlap their recording waits. At window 8, roughly 1 ms of aggregate write time per request coexists with roughly 8 ms client p50 latency. The counters do not report a write-operation count, so this experiment does not determine requests per actual write.

For reference, median run-level **mean recording-observation time** was 3.804 ms at window 1 and 7.845 ms at window 8. Do not subtract aggregate write time from summed overlapping request waits or subtract stage percentiles to derive another stage.

### Maximum write times

These are medians of the five **Archive lifetime maximum** counters sampled before and after the measured phase. They include warmup. The difference between the two is not the measured phase's maximum.

| Client window | Lifetime maximum before (ms) | Lifetime maximum after (ms) |
| --- | ---: | ---: |
| 1 | 6.009 | 12.767 |
| 8 | 5.433 | 10.564 |

## How collection works

1. The benchmark finds recorder counters by numeric counter type and Archive ID in the connected driver's `CountersReader`; labels are not used. Exactly one Archive must be present.
2. Before warmup, it verifies zero recorded bytes for this fresh, dedicated server.
3. It sends and drains 500 warmup requests, then waits for **32,000 bytes** in the recorder counter and saves the baseline tuple.
4. It measures 2,000 requests using the existing client timers, then waits for **160,000 cumulative bytes** and reads the final tuple.
5. It reports byte/time deltas and both lifetime maxima. Counter registration IDs, allocation state, types, and Archive keys are checked so closed or reused counters fail the diagnostic.

Our SBE cancel request uses one 64-byte recorded frame, including the Aeron header and alignment. The benchmark derives the frame size from the codec and rejects fragmentation for this diagnostic. Warmup/sample integration tests independently verify the resulting byte totals against a live server.

Archive publishes its bytes, total time, and maximum counters separately after recorder work. A reply can arrive before those statistics are published. Boundary collection therefore waits for the expected bytes and a tuple unchanged for 20 ms, with a five-second timeout. These waits are outside the latency, throughput, and CPU timers. This reduces publication races but does not provide an atomic snapshot or protection against an arbitrarily delayed counter update. The tool is for an idle dedicated recording environment, not shared-production attribution. Additional recording traffic causes an expected-byte mismatch.

## Method and validation

- 13:07:54–13:09:00 PDT, 1 October 2026.
- Apple M4 Pro, 14 logical CPUs, 24 GiB memory; macOS 26.6.2; Homebrew OpenJDK 25.0.4.1; Aeron 1.53.0 and Agrona 2.6.0.
- Five rounds with client windows 1 and 8, shuffled with seed `20261001`: **10 runs**, each using fresh server/client JVMs, a new empty Archive, and a unique temporary Aeron directory.
- Quiet server; engine log window 8; engine busy-spin polling; client sleeping polling; Archive file/catalog sync levels 2/2. Server/engine code and durability settings were unchanged.
- All runs reported 2,000/2,000 stage samples after 500/500 skipped warmup completions. All benchmark results were the expected missing-order cancellation response; UUIDs were fresh and requests were never resent.
- No other exchange JVMs existed before the experiment; none remained afterward. Builds/tests were not run concurrently with measurements. No CPU affinity or desktop isolation was applied.
- **508 tests passed**, zero failures/errors/skips, plus Spotless. The ten new counter tests cover warmup subtraction, lifetime maximum labeling, delayed statistics, absent/ambiguous Archive counters, closed/reused IDs, unexpected writes, timeouts, and decreasing totals. Existing process tests exercise the opt-in flag and real recorded byte counts.
- Source hashes match the verified build and remained unchanged during measurements. This experiment did not repeat the timing-off control; the new reader performs no per-request reads and changes only phase boundaries. The boundary settling pauses and ordinary desktop variability mean absolute values should not be treated as a controlled comparison with the September runs.
- This is a short closed-loop cancellation workload. It excludes populated-book matching, gRPC, the future Go backend, sustained offered-load behavior, and rare production tail events. The write-time ratio is elapsed time inside the recorder's timed operations, **not CPU utilization**.

## Reproduce

Use Java 25 and the usual JVM option `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED`.

Server program arguments:

```text
<new-archive-directory> --quiet --stage-timing=500,2000
```

Benchmark program arguments:

```text
500 2000 1 sleep --archive-counters
```

Repeat with window `8`, using a new server and Archive for every run. Stop the server with **⌃C** afterward to print its stage report. The optional fifth benchmark argument enables Archive counters; ordinary benchmark invocation remains unchanged.

## Saved evidence

- [Raw CSV](runs.csv) and [JSON](runs.json)
- [Summary statistics and full run ranges](summary.json)
- [Environment, run order, settings, source hashes](environment.json)
- [Counter semantics from the pinned dependency source](counter-semantics.json)
- [Implementation patch against the recorded base commit](implementation.patch)
- [Benchmark and server logs](logs/)
