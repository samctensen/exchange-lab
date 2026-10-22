# Gateway lifetime diagnostics

Start `WebSocketGatewayServer` with `--diagnostics` to print aggregate counters
after shutdown drains the gateway. The flag is off by default and must not have
a value. Repeated flags are rejected before opening resources.

```sh
java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "target/classes:$(cat target/runtime-classpath.txt)" \
  dev.sam.exchange.gateway.WebSocketGatewayServer --diagnostics
```

See [gateway operations](gateway-operations.md) for the build/classpath command
and other launcher options. Stop the gateway with **⌃C** while leaving the engine
running until accepted work has drained. The report includes that drain and all
earlier activity during this gateway's lifetime. An abrupt kill can prevent the
final report from being printed.

## Counters

| Counter | Meaning |
| --- | --- |
| Accepted | Requests admitted into the shared queue. |
| Queue full | Submissions rejected because the queue had no free slot. |
| Not running | Submissions rejected before start, during close, or after the worker stopped. |
| Activated | Accepted requests moved into the active window. |
| Completed | Active requests with a matching definitive engine result. |
| Business rejected | Completed results that are `RejectResult`; a subset of Completed. |
| Failed | Accepted requests ending in a terminal gateway/transport failure. |
| Unmatched responses | Responses discarded because no offered active request matched their UUID. |
| Successful offers | Offers accepted by Aeron, including accepted retry attempts. |
| Retries | Successful offers after an earlier accepted attempt for that active request. |
| Offer / reply timeouts | Expired send or reply phases; a reply timeout can trigger a retry. |

The queue high-water mark is an observed maximum: the worker can dequeue between
admission and sampling. The active high-water mark describes the active window.
Queue wait timings cover admission until activation, not total request latency.

## Interpret failures carefully

A business rejection is a definitive result. Failed requests can include work
that was never offered and work the engine may already have executed. The Failed
counter does not tell those cases apart.

Intermediate retry timeouts are not terminal failures. A request that times out
once and then receives its result counts as Completed once. Unmatched responses
may include duplicate or delayed replies; the counter does not identify their
cause. Diagnostics retain aggregates, not request UUIDs or order payloads.

Cancelling a caller's future does not cancel an admitted command. A later engine
result still belongs to the processing outcome and is counted. Retain request
identities separately when reconciling uncertain outcomes.

Snapshots copy the counters under a lock and remain unchanged as later activity
arrives. Formatting uses that copy outside the counter lock. A live snapshot can
show work in progress; use the final report to inspect a completed drain.
Diagnostics add measurement overhead when enabled, so compare performance with
the flag held constant or disabled.

[Health probes](gateway-health.md) describe current availability; these counters
explain what happened over the gateway's lifetime.

For live scraping, use [Prometheus metrics](gateway-metrics.md). `--metrics` and
`--diagnostics` are independent output options backed by the same collector.
