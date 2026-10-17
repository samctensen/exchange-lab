# Gateway health checks

The WebSocket gateway serves HTTP probes on the same loopback port as `/orders`.
Probes inspect local state. They do not submit orders, send Aeron probe messages,
consume replies, or append entries to the Archive.

| Path | Healthy response | Unhealthy response | What it observes |
| --- | --- | --- | --- |
| `/health/live` | `200`, `{"status":"live"}` | Connection failure if the listener is unavailable | The HTTP listener can answer. |
| `/health/ready` | `200`, `{"status":"ready"}` | `503`, `{"status":"not_ready"}` | The worker is running, the request publication is connected, and its request/reply resources are open. |

Use `GET` to read JSON or `HEAD` to inspect the same status and content length
without a body. Other methods receive `405` with `Allow: GET, HEAD`. Paths are
exact: query strings and trailing slashes are not alternative probe URLs.
Requests must have no body and pass HTTP decoding. Health responses disable
caching, and the server closes the HTTP connection after responding.
Requests exceeding the existing 16 KiB HTTP aggregation limit may receive `413`
before reaching the probe handler.

The existing local browser Origin policy also applies to probes. Native clients
such as curl may omit Origin. These checks add neither authentication nor TLS;
the listener remains bound to `127.0.0.1`.

## Check a running gateway

Start the engine and gateway using [gateway operations](gateway-operations.md).
If the gateway uses `--port=0`, replace `8080` with its printed port.

```sh
curl --fail-with-body http://127.0.0.1:8080/health/live
curl --fail-with-body http://127.0.0.1:8080/health/ready
curl --head http://127.0.0.1:8080/health/ready
```

Liveness succeeding while readiness fails means the listener is answering but
the gateway is not currently ready to send. Inspect the engine process, selected
Media Driver directory, and gateway logs. A disconnect becomes visible after
Aeron observes it; failure detection is not instantaneous.

## Limits

- The transport can disconnect immediately after a successful probe.
- A connected publication does not prove an order will be recorded, executed,
  or replied to. The response route can be created lazily, so readiness does not
  require an already connected response publication.
- A full queue can reject admission even while readiness returns `200`.
- Probes share the listener and connection limit with WebSocket clients. They
  are not a separate management service with reserved capacity.

During draining, `EngineGateway.isReady()` returns false. During normal server
shutdown, the HTTP listener closes before accepted engine requests finish
draining, so an HTTP probe may instead fail to connect. A failed probe does not
cancel an accepted order. Preserve its UUID and follow the
[reconciliation rules](websocket-contract.md#errors-and-uncertain-outcomes).

For embedded Java clients, `AeronRequestClient.isTransportAvailable()` observes
the transport resources only. `EngineGateway.isReady()` adds the worker lifecycle
check. Neither method changes admission state or consumes messages.
