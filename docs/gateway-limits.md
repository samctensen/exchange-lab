# Gateway resource limits

The loopback gateway has separate limits for TCP connections, each WebSocket,
and the shared engine queue. Change them with the launcher options below;
existing constructors and an unconfigured launcher retain these defaults.

| Option | Default | What it bounds |
| --- | --- | --- |
| `--max-connections=n` | `128` | Accepted TCP connections, including unupgraded sockets and HTTP probes. Excess connections close before upgrade. |
| `--max-pending-per-connection=n` | `32` | Outstanding requests on one WebSocket. A slot stays occupied until its response write completes. |
| `--max-message-bytes=n` | `16384` | Inbound WebSocket frame/message bytes, including the aggregate of fragments, and HTTP request body aggregation. |
| `--max-response-bytes=n` | `1048576` | Encoded JSON response bytes. This is separate from Aeron/SBE and engine reply budgets. |
| `--read-idle-timeout-ms=n` | `60000` | Time without inbound reads, before or after WebSocket upgrade. |
| `--handshake-timeout-ms=n` | `5000` | Time to complete an initiated WebSocket upgrade. It does not replace the read-idle timeout for a partial HTTP request. |

Counts and byte limits must be positive signed 32-bit integers. Timeouts must
be integer milliseconds in `1..9223372036854`, so converting to nanoseconds
cannot overflow. Missing, repeated, malformed and out-of-range options fail
before opening Aeron or a listener. There is no zero-value disable mode.

## Configure deliberately

For example, append these program arguments to the gateway launch command in
[gateway operations](gateway-operations.md):

```text
--max-connections=64 --max-pending-per-connection=16
--max-message-bytes=16384 --max-response-bytes=1048576
--read-idle-timeout-ms=30000 --handshake-timeout-ms=3000
```

Increasing `--queue-capacity` or `--max-in-flight` does not increase the
per-connection budget. Increasing the per-connection budget does not increase
the shared queue. A connection-limit rejection does not evict an existing
client. Closing a connection returns its slot, while accepted engine requests
continue to completion.

The inbound budget counts **UTF-8 bytes**, not Java characters, and applies to
both a whole text frame and a fragmented text message. Oversized WebSocket
messages close with code `1009`; oversized HTTP bodies can receive `413` before
routing. Binary order messages remain unsupported.

The response budget is checked after JSON encoding. It limits delivery size,
not the allocation needed to build a result. The unchanged outbound watermarks
are 64/128 KiB; an unwritable connection closes even when its reply is below the
response cap. Raising the JSON cap does not raise those watermarks, the engine's
reply queue budget, Aeron's maximum message length, or SBE's trade-group limit.

## Outcomes and operations

- A pending-budget rejection returns `RESOURCE_EXHAUSTED` with
  `outcomeUnknown:false`: that submission never entered the engine gateway.
- A response too large to deliver closes with `1009` and an unknown-outcome
  reason. The command may already have executed. Preserve and retry its original
  UUID and command; a new UUID can execute a new command.
- Read-idle closure and shutdown do not cancel admitted requests. A client must
  reconnect and reconcile requests for which it has no definitive response.
- Health checks and metrics share connection capacity with order sockets. They
  have no reserved management connections.

These controls bound resources; they are not authentication, account-level rate
limits or a capacity guarantee. The service still binds to `127.0.0.1`. See the
[WebSocket contract](websocket-contract.md#errors-and-uncertain-outcomes) for
reconciliation and the [health guide](gateway-health.md) for readiness limits.
