# Gateway Prometheus metrics

Add `--metrics` to the [gateway launch command](gateway-operations.md) to expose
aggregate diagnostics at `http://127.0.0.1:<port>/metrics`. It is off by default;
without the flag the exact path returns `404`. The flag takes no value and may
appear only once.

```sh
curl --fail-with-body http://127.0.0.1:8080/metrics
curl --head http://127.0.0.1:8080/metrics
```

The endpoint uses [Prometheus text format 0.0.4](https://prometheus.io/docs/instrumenting/exposition_formats/),
with `HELP` and `TYPE` metadata and LF-terminated samples. It returns
`text/plain; version=0.0.4; charset=utf-8`, disables caching and closes the HTTP
connection. `HEAD` reports the length of the representation without a body.

Only exact `GET /metrics` and `HEAD /metrics` requests are supported. Query
strings and trailing slashes are different paths. Unsupported methods receive
`405` with `Allow: GET, HEAD`; malformed requests or bodies receive `400`
(oversized bodies may receive `413` earlier). The same local browser Origin
policy applies as on the health probes. A scrape failure returns an empty `503`
without internal error details and does not disable subsequent scrapes.

## Exposed measurements

All names below have the prefix **`exchange_gateway_`**. There are no dynamic
labels, request UUIDs, order IDs or payloads in this export.

| Suffix | Type | Meaning |
| --- | --- | --- |
| `accepted_total` | Counter | Requests admitted to the shared gateway queue. |
| `queue_full_total` | Counter | Submissions rejected because that queue was full. |
| `not_running_total` | Counter | Submissions rejected because the gateway was not running. |
| `activated_total` | Counter | Accepted requests moved into the active window. |
| `completed_total` | Counter | Definitive engine results, including business rejections. |
| `business_rejected_total` | Counter | Completed `RejectResult` responses; a subset of completed. |
| `failed_total` | Counter | Accepted requests ending in terminal gateway/transport failure. |
| `unmatched_responses_total` | Counter | Engine responses without a matching offered active request. |
| `offers_total` | Counter | Successful publication offers, including retries. |
| `retries_total` | Counter | Successful offers after the first accepted attempt. |
| `offer_timeouts_total` | Counter | Attempts that expired before publication accepted an offer. |
| `reply_timeouts_total` | Counter | Attempts that expired while waiting for an engine result. |
| `ready` | Gauge | `1` when the worker is running and request transport is available; otherwise `0`. |
| `queue_high_water` | Gauge | Largest observed queued count over the gateway's lifetime. |
| `active_high_water` | Gauge | Largest active request count over the gateway's lifetime. |
| `queue_wait_seconds_total` | Counter | Total admission-to-activation wait for activated requests, in seconds. |
| `queue_wait_max_seconds` | Gauge | Largest admission-to-activation wait, in seconds. |

The measurements reset when the gateway process restarts. Queue high-water is
an observed maximum: a worker can dequeue before admission sampling. High-water
values are not current queue depths. Queue-wait values are neither full request
latency nor percentiles. Divide a rate of `queue_wait_seconds_total` by a rate
of `activated_total` to estimate mean queue wait over the same interval, taking
care with intervals that have no activations.

## Read-only observation

Scrapes copy the existing counters and read availability. They do not submit
commands, poll replies or append to Archive. Metrics remain readable with
`ready 0` while an engine request subscription is unavailable. Readiness is
sampled separately from the counter snapshot; it is not an atomic system-wide
view or a guarantee that the next request will succeed. A full queue can reject
work while `ready` is still `1`.

`--metrics` enables collection without printing a shutdown report.
`--diagnostics` prints the report without exposing `/metrics`. Use both flags
for both outputs; they share one collector. See [diagnostics](gateway-diagnostics.md)
for failure, retry and reconciliation semantics. HTTP/JSON validation failures
and per-socket pending rejections happen before the shared gateway, so they do
not increment its admission/failure counters.

An example local Prometheus scrape job is:

```yaml
scrape_configs:
  - job_name: exchange-gateway
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:8080']
```

Run the scraper in the same network namespace as the loopback listener.
Metrics share connection capacity with clients and [health probes](gateway-health.md);
there is no reserved management capacity. During shutdown the listener closes
before accepted work finishes draining, so the final scrape may miss completions.
Use the shutdown report when the completed drain matters. Collection and
formatting add overhead when enabled; keep the flags consistent during benchmarks.
