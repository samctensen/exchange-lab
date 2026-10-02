# Full gRPC path: concurrency, queue pressure, and deadlines

> Historical measurement: the gRPC adapter and benchmark launcher have been retired in
> favor of the WebSocket gateway. These results describe the earlier implementation, not
> current WebSocket performance. The merged gRPC code is preserved at commit
> `ff7b85b46bb74fa320aa49b5ac87adb3f8b44759`; use a separate checkout of that revision
> when reproducing it. Saved data and reproduction artifacts below are retained.

## Conclusions

**The larger-window throughput gain survives the gRPC gateway.** Matching client, gateway, and engine concurrency at 32 delivered about 4,018 successful requests/s with 7.82 ms median latency and 8.87 ms p99. At 8, throughput was about 1,031/s. All 50,000 measured RPCs in the 25 normal comparison runs succeeded.

Increasing only client concurrency mostly added queueing. With client 32 and gateway/engine 8, median latency rose to 31.92 ms and p99 to 35.89 ms, while throughput stayed near 1,008/s. Raising both backend windows to 32 removed most of that waiting.

The pressure probes exposed the next design priority: **overload and RPC deadlines need explicit admission semantics**. The bounded queue rejected excess work, but reports it as generic `UNAVAILABLE`. In three cold runs with a 10 ms RPC deadline, no RPC reported success, yet **905 accepted commands completed in the engine**. RPC failure cannot be treated as proof that a command did not execute.

The default remains eight. Window 32 is the strongest tested option for this cancellation workload; use the configuration flags to reproduce it. A production default still needs a latency/error target and sustained offered-load testing, including real matching work.

## Normal comparison

Each cell is the median of five per-run statistics. Percentiles are not pooled. The gateway queue capacity was 128 and the RPC deadline was five seconds throughout this table.

| Engine window | Gateway window | Client outstanding | Successful requests/s | Client p50 (ms) | Client p99 (ms) | Per-run p99 range (ms) |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 8 | 8 | 8 | 1,030.9 | 7.892 | 9.416 | 8.186–10.089 |
| 16 | 16 | 16 | 2,050.4 | 7.880 | 8.919 | 8.476–9.081 |
| 32 | 32 | 32 | 4,018.3 | 7.822 | 8.873 | 8.320–9.571 |
| 8 | 8 | 32 | 1,007.9 | 31.925 | 35.893 | 33.044–37.906 |
| 16 | 16 | 32 | 2,029.3 | 15.882 | 17.793 | 16.208–21.902 |

Within matched rounds, 32/32/32 delivered 3.77–3.98 times the successful throughput of 8/8/8 (median ratio 3.93). With client concurrency fixed at 32, increasing the gateway/engine limits from 8 to 32 delivered a median within-round throughput ratio of 3.98 and p99 ratio of 0.25. The fixed-client control changes two backend limits together; it does not independently isolate the gateway limit from the engine limit.

There were **zero RPC failures, queue-full rejections, gateway retries, gateway offer timeouts, or gateway reply timeouts** across these normal runs. Each run recorded 2,500 fresh requests: 500 warmup and 2,000 measured requests.

These results are consistent with the earlier [direct-IPC comparison](../2026-10-01-aeron-log-windows/report.md). The two experiments use different client loops and were not paired to isolate protocol overhead; do not subtract their percentiles to claim an exact gRPC cost.

### Queue evidence

Diagnostics below include warmup and shutdown drain. They are lifetime aggregates, not measurements limited to the benchmark's timed phase.

| Engine/gateway | Client | Mean queue wait, median across runs | Observed queue high-water, median |
| --- | --- | ---: | ---: |
| 8/8 | 8 | 0.011 ms | 7 |
| 16/16 | 16 | 0.021 ms | 15 |
| 32/32 | 32 | 0.039 ms | 31 |
| 8/8 | 32 | 22.900 ms | 31 |
| 16/16 | 32 | 6.975 ms | 31 |

Matching the three windows keeps average gateway queue wait small. More outstanding calls than active backend capacity creates sustained waiting. A high-water mark alone cannot distinguish a brief arrival burst from a long queue: the mean-wait measurements provide that additional context.

The worker can dequeue between an enqueue and the queue-size sample, so observed high-water can miss a transient peak. Active high-water is measured by the owning gateway worker and never exceeded the configured active limit.

## Queue-pressure probes

Three runs per backend window, client concurrency 256, queue capacity 128, five-second RPC deadline, 2,000 calls, **zero warmup**. These are cold overload probes, including connection setup/JIT effects. Their success latencies and throughputs are not steady-state capacity or production latency estimates.

| Engine/gateway window | Queue-full rejections, median / 2,000 | Rejected calls, median | Rejection range | Observed queue peak |
| --- | ---: | ---: | ---: | ---: |
| 8/8 | 1,719 | 85.95% | 85.20–86.25% | 128 |
| 16/16 | 1,535 | 76.75% | 75.50–78.40% | 128 |
| 32/32 | 1,222 | 61.10% | 58.45–61.55% | 128 |

Every queue-full event matched a client `UNAVAILABLE` response; all accepted requests received successful RPC replies. No RPC deadlines or gateway attempt timeouts occurred in these pressure runs. Queue capacity and active-request bounds held, and accepted work drained on shutdown.

Median successful-call p99s were 327.3, 262.5, and 240.5 ms respectively, including cold-start effects. The queue provides a finite admission buffer, not a latency guarantee. These are closed-loop calls: fast rejections immediately release client slots and permit more submissions, which can sustain rejection pressure. This is not a fixed-rate arrival experiment.

## Short-deadline probes

Three cold runs: engine/gateway 8, client 32, queue capacity 128, **10 ms RPC deadline**, zero warmup, 2,000 calls each.

| Run | RPC successes | `DEADLINE_EXCEEDED` | `UNAVAILABLE` / queue full | Accepted and completed by engine |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0 | 755 | 1,245 | 296 |
| 2 | 0 | 767 | 1,233 | 305 |
| 3 | 0 | 763 | 1,237 | 304 |

All 6,000 RPCs failed from the caller's perspective. Nevertheless, **905 commands were accepted, offered once to Aeron, recorded, processed, and replied to by the engine**. Gateway diagnostics matched the engine's completed fresh-request samples after shutdown drain. Gateway retries and offer/reply timeouts were zero: the short client deadline and the gateway's five-second attempt deadline are different clocks.

Some RPCs expired before gateway admission; deadline counts are therefore not engine-command counts. The evidence establishes engine completion despite failed RPC outcomes. Aggregate diagnostics do not determine whether each command executed before or after its particular client deadline.

This is an intentionally severe cold-start probe, not an estimate of the production failure probability at 10 ms. It demonstrates the existing semantics: gRPC cancellation does not remove already-accepted gateway work, and a lost/timed-out reply leaves the caller uncertain. Faster refill after timeouts can also add pressure while earlier accepted work continues.

## What changed

- `GrpcGatewayServer` accepts `--max-in-flight`, `--queue-capacity`, `--port`, and `--diagnostics`. Defaults remain 8 active, 128 queued, port 50051, diagnostics off. Port zero requests an ephemeral port. Invalid/duplicate arguments fail before connecting to Aeron.
- `GrpcLatencyBenchmark` sends fresh missing-order cancellations through the real protobuf/gRPC service using asynchronous futures. A bounded completion queue refills slots in completion order. Each request has a fresh UUID and one application RPC; channel retries are disabled. The gateway retains its existing five-second, three-attempt Aeron retry policy, with retries counted separately.
- Reports separate successful throughput from all-completion throughput, successful latency from terminal latency, and each non-OK gRPC status. Unexpected request IDs or business results abort the run; warmup failures also abort. Measured RPC failures remain in the report so overload is visible. A successful process exit therefore means the diagnostic completed, not that every RPC succeeded.
- Optional gateway diagnostics count accepted work, queue-full rejection, activations, queue/active high-water, mean/max queue wait, successful offers, retries, and offer/reply attempt-deadline expirations. Queue wait starts before the submission admission lock and ends at dequeue, excluding SBE encoding. Counters are synchronized and include warmup and shutdown drain.
- Engine FIFO execution, single-writer ownership, Archive file/catalog sync **2/2**, command semantics, gRPC error mapping, cancellation behavior, and default limits retain their prior behavior.

## Method and validation

- 13:48:20–13:50:00 PDT, 1 October 2026.
- Apple M4 Pro, 14 logical CPUs, 24 GiB RAM; macOS 26.6.2; Homebrew OpenJDK 25.0.4.1. Dependency versions and source hashes are recoverable from the recorded base commit and implementation patch.
- **37 runs:** five shuffled rounds of five normal configurations, then three rounds of three pressure configurations and one deadline probe. Shuffle seed `20261001`.
- Separate client, gateway, and engine JVMs; plaintext gRPC over localhost TCP with an ephemeral port. Fresh Archive and temporary Aeron directory per run. Gateway and engine share that directory. Engine busy-spin polling and gateway sleeping polling retain their existing strategies.
- Normal runs use 500 warmup and 2,000 measured requests. Probes use zero warmup and 2,000 calls. All commands are fresh-UUID `CancelOrder(1)` against an empty book. This exercises transport, mapping, queueing, logging, and missing-order cancellation, not populated-book matching or a Go backend.
- Per-request timing starts after request construction, before the stub call, and ends in the future completion listener. Phase throughput includes construction, submission, and completion handling. Channel setup, warmup, reporting, and shutdown are outside the normal measured phase; with zero warmup, establishing the asynchronous connection occurs inside the first measured calls.
- Stage diagnostics and gateway diagnostics were enabled throughout. No diagnostics-off control, CPU affinity, or desktop isolation. This experiment does not attribute overhead to individual network/serialization layers or measure total CPU cost.
- **552 tests passed**, zero failures/errors/skips, plus Spotless. Coverage includes reverse-order completion with bounded outstanding RPCs, fresh IDs across phases, status/deadline counting, incorrect replies, percentile/success-throughput calculations, all-failure reporting, invalid CLI input, queue-full/lifecycle distinction, retries versus offer/reply timeouts, and three real full-path process runs at 8/16/32. The new process tests failed against the old launcher before implementation.
- Full verification ran before measurements in an isolated source copy. Source hashes matched the verified build and remained unchanged. No other exchange JVMs existed before the experiment; none remained afterward. No builds/tests ran concurrently with measurements.
- Every run respected gateway queue and active bounds. Every accepted request matched a completed engine stage sample after warmup accounting and shutdown drain. No gateway retry or attempt timeout occurred in any measured experiment; controlled tests exercise those diagnostic branches.

## Reproduce

Use Java 25 and `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED` for the processes. Engine and gateway must use the same `java.io.tmpdir`.

Engine program arguments:

```text
<fresh-archive-directory> --quiet --log-window=32 --stage-timing=500,2000
```

Gateway program arguments:

```text
--max-in-flight=32 --queue-capacity=128 --port=50051 --diagnostics
```

Benchmark program arguments (warmup, samples, outstanding calls, RPC deadline milliseconds, port):

```text
500 2000 32 5000 50051
```

Stop the gateway with **⌃C** after the benchmark, let its accepted work drain, then stop the engine to print its stage report. For pressure probes use client arguments `0 2000 256 5000 50051`; for the short-deadline probe use `0 2000 32 10 50051` with gateway/engine windows 8. Set engine stage timing to `0,2000` for zero-warmup probes and always start fresh processes/Archive.

## Next lesson

Implement explicit admission outcomes and cancellation handling at the gateway boundary:

1. Distinguish queue-full overload from transport/engine failure in the gRPC contract.
2. Carry RPC cancellation/deadline information into queued work, so expired work can be removed before it is published to the engine.
3. Preserve the boundary after publication: a timeout cannot undo a durable command. Treat the outcome as unknown and use the same request UUID when resolving/retrying it.
4. Test races between cancellation, dequeue, and publication, then repeat the pressure/deadline probes.

Keep queue capacity bounded. Choose a latency/error target and run a sustained offered-load experiment before raising the application default.

## Evidence

- [Raw CSV](runs.csv) and [JSON](runs.json)
- [Summary statistics, ranges, and within-round ratios](summary.json)
- [Environment, run order, validation, and source hashes](environment.json)
- [All 111 engine/gateway/benchmark logs](logs/)
- [Implementation patch](implementation.patch) against the recorded base commit
- [Exact experiment runner](runner.py); adapt its local repository/build paths and use a new output directory to rerun
