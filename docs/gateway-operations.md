# Run and configure the WebSocket gateway

`WebSocketGatewayServer` exposes `ws://127.0.0.1:<port>/orders` and connects to an
existing engine's Aeron Media Driver. Start `AeronEngineServer` first. The gateway
binds only to loopback and has no authentication or TLS.

See the [WebSocket contract](websocket-contract.md) for JSON messages, browser
origins, connection limits, and retry rules.

## Launch from a terminal

Run these commands from the repository root with Java 25 and Maven 3.9+. Confirm
both `java -version` and `mvn -v` report Java 25. Build the classes and runtime
classpath once:

```sh
mvn -q compile dependency:build-classpath \
  -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt
```

In the first terminal, start the engine with an explicit temporary directory for
its Media Driver and a separate persistent Archive directory:

```sh
mkdir -p /tmp/exchange-lab-demo
java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "target/classes:$(cat target/runtime-classpath.txt)" \
  dev.sam.exchange.transport.AeronEngineServer data/demo-archive --quiet \
  --aeron-directory=/tmp/exchange-lab-demo/exchange-lab-aeron
```

Wait for `Server ready:`. Reusing `data/demo-archive` recovers the same book and
request history; choose a new Archive directory for a fresh exchange. Keep the
Archive outside the disposable Media Driver directory, and run only one engine
owner for each directory.

The engine rejects a blank Archive argument or driver path before starting
resources. It also refuses an Archive directory inside the disposable driver
directory, including paths that resolve there through symlinks. The driver
directory is removed during normal shutdown; the Archive is retained.

In a second terminal, connect the gateway to that Media Driver:

```sh
java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "target/classes:$(cat target/runtime-classpath.txt)" \
  dev.sam.exchange.gateway.WebSocketGatewayServer \
  --aeron-directory=/tmp/exchange-lab-demo/exchange-lab-aeron \
  --port=8080 --queue-capacity=128 --max-in-flight=8 \
  --timeout-ms=5000 --max-attempts=3
```

Wait for `WebSocket gateway listening on ws://127.0.0.1:8080/orders`, then connect
the client. The gateway does not start an engine or Media Driver. Its explicit
`--aeron-directory` allows the two JVMs to have different `java.io.tmpdir` values.
With no directory flag, both JVMs must resolve the same default Media Driver path.

Use `--port=0` to select an available port and read the actual endpoint from the
startup output. Multiple gateways can share an engine with different ports; each
has its own Aeron response route.

HTTP `/health/live` and `/health/ready` are available on that same port. Use them
to check the listener and gateway availability without sending engine commands.
See [health checks](gateway-health.md) for curl commands, response codes, and
the limits of a successful readiness probe.

In Zed, the class play button can launch the same main classes. Supply the gateway
flags as program arguments and keep the `--add-opens` JVM option. **⌘J** switches
between the editor and its terminal.

## Gateway options

All value options use `--name=value`. Unknown options, repeated options, missing
values, and invalid values fail before connecting to Aeron or opening a socket.

| Option | Default | Meaning |
| --- | --- | --- |
| `--port=n` | `8080` | Loopback listener port, `0..65535`; zero selects an available port. |
| `--queue-capacity=n` | `128` | Maximum queued requests waiting for an active slot. Positive 32-bit integer. |
| `--max-in-flight=n` | `8` | Maximum active requests being offered or awaiting replies. Positive 32-bit integer. |
| `--aeron-directory=path` | `java.io.tmpdir/exchange-lab-aeron` | Existing engine Media Driver directory; a nonblank path. This is not the Archive directory. |
| `--timeout-ms=n` | `5000` | Timeout for each send phase and each reply wait. Integer milliseconds, `1..9223372036854`, so conversion to nanoseconds cannot overflow. |
| `--max-attempts=n` | `3` | Total attempts including the first; `1` disables automatic retries after a missing reply. Positive 32-bit integer. |
| `--diagnostics` | Off | Print aggregate lifetime counters after shutdown drains accepted work; takes no value. |
| `--help` | — | Print usage and exit without starting resources; use by itself. |

Print help without starting the engine:

```sh
java -cp "target/classes:$(cat target/runtime-classpath.txt)" \
  dev.sam.exchange.gateway.WebSocketGatewayServer --help
```

Queue capacity and the active window are separate limits. Increasing them does
not change the WebSocket contract's per-connection pending limit or message limits.
The timeout starts when a request becomes active; time spent waiting in the queue
is additional. Each attempt gets a send deadline and, after publication accepts
it, a reply deadline. Temporary offer back pressure is retried within the send
deadline. Missing replies trigger another attempt up to `--max-attempts`.

These are gateway client controls. They do not change the engine's recording
deadlines or its separate `--reply-timeout-ms`, `--reply-capacity`, and
`--reply-bytes` settings. See [engine reply controls](../README.md#reply-limits-and-diagnostics).

Enable `--diagnostics` to report admission, completion, business rejection,
terminal failure, and retry counters after shutdown. The report includes work
that finishes during the drain and retains no request payloads. See
[gateway diagnostics](gateway-diagnostics.md) for counter definitions and how
to interpret uncertain outcomes.

## Stop and reconcile

1. Stop the gateway with **⌃C** while the engine remains running. Shutdown closes
   the listener and client sockets, then drains accepted requests before closing
   Aeron. Clients may disconnect before receiving their results.
2. Wait for the gateway process to exit, then stop the engine with **⌃C**. Larger
   queues, timeouts, and attempt limits can make the gateway take longer to drain.
3. Retain every request without a definitive result. On reconnect, resend the
   **same UUID and identical command**. The engine's recorded history restores
   request deduplication across restarts.

A timeout, lost connection, or shutdown does not cancel an accepted order. An
`outcomeUnknown:true` response means execution may already have happened. A new
UUID represents a new request and can execute again. Even `--max-attempts=1` can
leave an unknown outcome; it only limits automatic retries.

### Embedded gateway lifecycle

Code that embeds `EngineGateway` must keep its borrowed Aeron client, publication,
and subscription open until `gateway.close()` finishes. Close stops admission and
waits for the worker to drain accepted requests. If the closing thread is
interrupted, it still waits for the worker to finish, then restores the interrupt
flag and throws `IllegalStateException` with the interruption as its cause. This
keeps try-with-resources from closing Aeron beneath an active worker.

An unexpected worker failure completes all active and queued request futures
exceptionally. Their exceptions retain the original failure as the cause; later
submissions are rejected with that same cause. Check the exception chain in Java
logs when diagnosing transport failures. These diagnostics do not change the
WebSocket error contract or make an uncertain order safe to submit under a new UUID.

### Direct Aeron clients

The standalone `AeronEngineClient` demo and `AeronLatencyBenchmark` accept the same
`--aeron-directory=path` selection as the gateway. This lets each process use its
own temporary directory while connecting to one driver. Both provide `--help`
without requiring a running engine; see the [demo client options](../README.md#run-the-ipc-demo-in-zed)
and [benchmark instructions](../README.md#measure-ipc-latency-and-throughput).

`AeronRequestClient.send` is the blocking helper for direct Java integrations;
the demo uses `EngineGateway` to submit its batch. Interrupting the blocking
call's thread stops waiting and preserves the interrupt flag. Closing its reply
subscription also ends the wait promptly. Once any attempt has been accepted by
the publication, either failure reports the request UUID and an unknown outcome.
Keep that UUID and command for reconciliation; stopping a wait does not cancel
the order.

The nonblocking `trySend` and `pollResponses` methods reject a closed reply
subscription. `pollResponses` also rejects a null callback or negative fragment
limit before consuming replies. A zero limit is valid: it drains only responses
already decoded into the client's queue, without reading new Aeron fragments.

When embedding `AeronEngineServer.main`, its shutdown hook is removed after main
returns or startup fails. During JVM shutdown the hook still requests an orderly
stop and waits for resource cleanup.

## Common startup and request problems

| Symptom | Check |
| --- | --- |
| No active Aeron driver / connection timeout | Start the engine, wait for `Server ready:`, and use that exact directory with `--aeron-directory`. |
| Listener port is in use | Choose another `--port`, or use `--port=0` and the printed endpoint. |
| `RESOURCE_EXHAUSTED` | Back off and retry the same UUID/command; either the shared queue or the connection's pending limit may be full. |
| `UNAVAILABLE` with an unknown outcome | Keep the request identity, restore the connection, and retry the same command. |
| Gateway takes time to stop | Keep the engine running while accepted work drains; configured limits are not a single overall shutdown deadline. |

For every request error, follow the [contract's outcome rules](websocket-contract.md#errors-and-uncertain-outcomes).
