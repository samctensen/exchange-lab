# Exchange Lab

This is my hands-on exploration of modern Java backend programming and high-frequency trading architecture, starting with the very basics.

I'm building a small exchange engine to refresh my Java fundamentals and understand order books, persistence, and messaging. The longer-term direction is a deterministic engine running with Aeron Cluster.

## Where it is today

The project currently uses plain Java 25, Maven, and JUnit 5:

- Order validation with prices in integer ticks and quantities in lots.
- An in-memory order book with add, find, cancel, and best bid/ask selection.
- Arrival-order priority between orders at the same price.
- Remaining-quantity tracking, partial fills, and removal of fully filled orders.
- A matching loop that consumes crossing orders and executes trades at each resting order's price.
- Typed place, cancel, and rejection results, with immutable trade lists and book snapshots.
- A single ordered SBE request log in Aeron Archive, with recording confirmation before execution.
- Separate Aeron IPC client and server processes, with UUIDs connecting requests to replies.
- Request deduplication across client reconnects, clean restarts, and abrupt process restarts.
- Bounded client retries after reply timeouts, reusing the same UUID and command.
- A bounded gateway queue and in-flight window, with one worker owning Aeron offers, reply polling, and UUID correlation.
- A loopback WebSocket gateway with strict JSON orders, bounded connections and requests, and reconnect-safe request IDs.
- A server that stays available between client sessions and closes its resources on a shutdown request.

This is a learning implementation. The book indexes bid and ask price levels separately,
preserves FIFO priority within each level, and supports direct lookup by order ID.
The server processes commands on one thread.

See [the current exchange architecture](docs/architecture.md#current-architecture) for the existing process boundaries.

The browser/mobile path is WebSocket → Java gateway → Aeron/SBE → engine. See
[the WebSocket contract guide](docs/websocket-contract.md) for the local endpoint,
messages, retry semantics, and production work still ahead. Backend services can use
the same WebSocket contract; the previous gRPC adapter and protobuf generation have been removed.

## How a request moves through the system

```text
Client: CommandRequest(UUID, EngineCommand)
  -> SBE encode -> Aeron IPC stream 1
  -> Server agent: SBE decode, then check the UUID cache
  -> New UUID: SBE encode -> server-owned IPC stream 2001 -> Archive
  -> Wait for Archive recording position >= offered request end position
  -> RequestStateMachine: validate, match/cancel, and cache the result
  -> CommandResponse(same UUID, CommandResult)
  -> SBE encode -> Aeron IPC stream 2
  -> Client: SBE decode, then complete the pending request with the matching UUID
```

A duplicate order ID that is still on the book produces a `RejectResult` without changing the book. First-time requests are recorded even when rejected, so replay can reconstruct the same response. Cancelling a missing order returns `CancelResult` with `cancelled=false`.

The server remembers each completed request's UUID, command, and response. Retrying the same UUID and command returns the original response without another recording append or order-book mutation. Reusing a UUID with a different command returns `REQUEST_ID_CONFLICT` and preserves the original cached entry. Recording failures and timeouts stop the agent before applying the pending request or caching a response.

After a reply timeout, the client resends the same request, up to three total attempts by default. A delayed matching reply completes the request; replies without an active matching request are ignored. If all attempts go unanswered, that request fails with its UUID and an unknown outcome, since the server may already have processed it. The gateway still drains other accepted requests before a successful close returns. In the IPC demo, both orders are queued before waiting for results, and an unknown outcome produces a nonzero process exit. A send failure on a later attempt also preserves the request UUID and unknown-outcome diagnostic.

The WebSocket gateway allows 128 queued requests and eight active requests. Its worker polls replies,
checks deadlines, admits queued work, and attempts nonblocking offers. Each active request keeps
its encoded bytes, UUID, attempt count, and deadline. The engine executes one recorded FIFO head
at a time; its bounded log window lets requests wait for Archive together while the gateway
keeps earlier replies in flight.

### Ordering, recording, and recovery

`ArchiveRequestLog` uses one server-owned `ExclusivePublication`. The server chooses the processing order across client sessions and writes that order into one recording. Recording each client's ingress session separately would not preserve a shared order across those sessions.

A successful `offer()` returns an end position in Aeron's publication buffer. It does **not** confirm persistence. `AeronEngineAgent` retains the request and that position until the corresponding `RecordingPos` counter catches up. Offer back pressure and recording catch-up each have their own fixed five-second deadline; the agent never offers an accepted request again while waiting. Only then does it call `RequestStateMachine.process()` and prepare a reply.

The live `ArchiveRuntime` sets both file and catalog sync levels to **2**. Archive forces data and metadata before advancing the recording counter. This adds disk latency to the request path. Storage still depends on the OS/filesystem/device honoring those operations; there is no replica or disk-failure protection. The standalone Archive demo retains sync level zero for its introductory normal-restart lesson.

Startup finds the single recording tagged `exchange-request-log` on stream **2001**, streams its SBE requests over replay stream **2002** into a fresh `RequestStateMachine`, then extends that same recording from its saved stop position. It restores the book, cached successful replies, and cached business rejections before opening the live request subscription. Recovery tolerates retries through the same deduplication logic; live retries and UUID conflicts do not append. An active, ambiguous, or truncated-prefix recording fails startup rather than guessing which history to apply.

If the server stops after recording a request but before applying it or delivering its reply, recovery applies it. The client must retry an uncertain outcome using the **same UUID and command**. This is replay-safe execution with correlated retries; it does not guarantee that a reply is delivered exactly once.

Read the implementation in this order:

1. `RequestStateMachine`: pure request execution and deduplication, with no persistence calls.
2. `ArchiveRequestLog`: catalog lookup, replay, extension, publication offers, and recording progress.
3. `AeronEngineAgent.doWork`: receive → offer → await recording → apply → reply.
4. `AeronEngineServer`: start Archive, recover, start the runner, and close resources in reverse order.

## Run locally

Install JDK 25 and Maven 3.9+, and make sure `mvn -v` reports Java 25.

```sh
mvn test             # Run the tests
mvn verify           # Build, test, and check formatting
mvn spotless:apply   # Format Java sources
```

### Run the WebSocket gateway in Zed

1. Run `AeronEngineServer.main` and leave it running.
2. Run `WebSocketGatewayServer.main` and wait for `ws://127.0.0.1:8080/orders`.
3. Submit JSON orders using [the browser example and contract](docs/websocket-contract.md).
4. Stop the gateway with **⌃C** before stopping the engine.

This endpoint is local-only and has no authentication or TLS. Each gateway has its own
Aeron response route; use different `--port` values when running multiple gateway instances.
The engine queues replies separately for each Aeron client connection. A stalled gateway does
not hold up other gateways' replies or recorded command execution. Each reply queue allows up to
64 responses and 64 KiB of encoded payload by default, with a fixed five-second deadline per response.
Overflow or expiry disables that connection's reply route; it does not undo accepted commands.
Reconnect and retry the same UUID and command to recover the original result.
An individual result larger than the configured byte budget also exceeds this queue limit.
Replies must additionally fit Aeron's publication message limit and SBE's trade-group limit.
Exceeding either disables that reply route without stopping other clients or undoing the order.
Reconnecting alone cannot make an oversized result fit; see the limits below.

Zed's class play button supplies Maven's `exec.args` when launching Java. The SBE generation
execution pins its own schema argument so those launch arguments do not replace the schema path.

### Run the IPC demo in Zed

1. Run `AeronEngineServer.main` and leave it running.
2. Run `AeronEngineClient.main` in another process. It sends a ten-lot bid and a four-lot ask, then prints their results.
3. Stop the server with **⌃C** in its terminal when finished. Its shutdown hook requests that the processing loop stop and waits for resource cleanup.

The server defaults to the persistent directory `data/archive`, which is ignored by Git. Its first program argument selects another Archive directory, for example `data/demo-archive --quiet`. Missing directories are created. Reuse the same directory to recover the same exchange; use a new directory for a fresh book. The directory contains Archive's catalog and recording segments, and must have one server owner.

The text journal implementation and its old CLI have been removed. Old `.journal` files are left untouched and are **not imported automatically**; passing a journal file where a directory is expected fails startup. Old journal data remains accessible through Git's previous implementation if a migration is needed.

After an abrupt kill, Archive's mark-file heartbeat may prevent an immediate restart for about ten seconds. Wait for that ownership check to expire; do not delete a live Archive's mark file. Normal shutdown closes the mark file and permits immediate restart.

The sample client uses fixed order IDs. Running it again against the same book can therefore produce a duplicate-ID rejection. A clean demo run can use a new Archive directory.

Both processes must use the same `java.io.tmpdir`, where the server creates `exchange-lab-aeron`. Aeron requires this JVM option:

```text
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
```

The project configures it for Maven tests and Zed terminals. Include it in the launch configuration when running elsewhere. Use matching client/server versions: IPC stream 1 carries SBE `PlaceOrderRequest` and `CancelOrderRequest` messages; stream 2 carries SBE `PlaceOrderResponse`, `CancelOrderResponse`, and `RejectOrderResponse` messages. Both live streams use the binary protocol instead of the old text wrappers.

`AeronEngineServer.main` owns recovery, Archive/Aeron resources, and shutdown. An Agrona `AgentRunner` calls `AeronEngineAgent.doWork()` and handles idling on the dedicated `exchange-engine` thread. Each pass discovers connections, tries one queued reply per connection, fills the bounded log window, and applies at most one recorded FIFO head. Replies retain immutable results and their original deadlines; the agent encodes each response into its shared buffer immediately before offering it. Matching remains single-threaded, while reply delivery progresses independently for each connection. Main closes the runner before finishing the log and closing Archive/driver. Recording failures stop the worker and are rethrown by main after cleanup, preserving their original cause. Reply expiry closes only its route. Tests cover shutdown, slow-client isolation, queue overflow, reconnect retries, recording failures, and recovery after SIGKILL. See [the architecture walkthrough](docs/architecture.md#follow-one-order) for the complete path.

#### Reply limits and diagnostics

Add these to the engine server's program arguments as needed:

| Option | Default | Controls |
| --- | --- | --- |
| `--reply-capacity=64` | 64 | Maximum queued replies per Aeron connection |
| `--reply-bytes=65536` | 65,536 | Total encoded bytes allowed in that connection's reply queue |
| `--reply-timeout-ms=5000` | 5,000 ms | Deadline from result preparation to successful publication offer |
| `--reply-stats` | Disabled | Print delivery counters after shutdown |
| `--help` | — | Print all server options and exit; use by itself |

For example: `data/demo-archive --quiet --reply-capacity=128 --reply-bytes=262144 --reply-timeout-ms=10000 --reply-stats`.
Count/byte limits and timeouts must be positive; timeouts must fit in a signed 64-bit nanosecond value.
The server validates the complete argument list before allocating timing buffers or opening resources.
These controls are independent of the five-second log-offer and recording deadlines.

With `--reply-stats`, stop the engine with **⌃C** to see a summary such as:

```text
Reply delivery: offered=12, expired=2, capacity=3, disconnected=0, unavailable=0, unencodable=0
```

`offered` counts successful Aeron offers, including cached retries; it does not confirm client receipt
or count unique executions. The other fields count discarded replies by cause, including the rest
of a queue when its route is disabled. `capacity` means the count or byte budget was exceeded;
`unavailable` includes terminal publication failures and requests whose route has already gone away;
`unencodable` means the SBE or publication message limit was exceeded. Replies still queued when the
server stops are not counted as discarded. Stats are optional, written on the engine thread, and
read only after that thread stops.

Increasing the byte budget can admit larger replies only within the publication and schema limits.
The current SBE response allows at most 65,534 trades (2,097,132 encoded bytes), and the publication's
maximum message length can be smaller. Splitting larger results across messages remains future work.
After a route is disabled, restart the affected gateway and retry the same UUID and command to recover
a deliverable cached outcome. A browser reconnect alone does not recreate the gateway's Aeron route.

The engine runner uses `BusySpinIdleStrategy`, following [Aeron’s guidance for low-latency subscribers](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide#application-threads). It keeps polling when idle and can consume roughly one CPU core. Budget a dedicated core for this approach in production; this demo leaves CPU placement to the operating system and does not reserve or pin a core. Stop the server with **⌃C** when finished.

### Run the response-channel demo in Zed

Run [`AeronResponseChannelDemo.main`](src/main/java/dev/sam/exchange/transport/AeronResponseChannelDemo.java)
with the same `--add-opens` JVM option. It starts its own embedded driver with a unique directory,
two logical clients, and an echo server in one JVM. Each client sends one short ASCII message.

```text
Client A received: [A]
Client B received: [B]
Reply isolation verified: each client received only its own echo.
```

Both clients share request stream 1 and response stream 2. The routing comes from
[Aeron response channels](https://github.com/aeron-io/aeron/wiki/Response-Channels):

1. Each client creates a subscription with `control-mode=response`.
2. Its request publication sets `response-correlation-id` to that subscription's `registrationId()`.
3. The server sees a separate request `Image` for each client and creates a response publication
   with `control-mode=response` and `response-correlation-id` set to that image's `correlationId()`.
4. The server echoes each message through its source image's response publication. Aeron routes
   it to the associated client; the client does not filter messages by their text.

These IDs describe live transport connections. Our persistent request UUIDs serve a different purpose:
recognizing retries even after reconnection or recovery. The live engine, gateway, demo client, and
latency benchmark use this response-channel setup too. `AeronEngineAgent` registers and removes routes
on its own thread and keeps each request's route through the log window and cached-reply path.
Only the command request is archived; a retry after reconnect uses the new connection's route.
The standalone demo uses five-second deadlines, sleeping waits, and automatic resource cleanup.
It is not a latency benchmark.

### Measure order-book performance

The [standalone JMH benchmarks](benchmarks/README.md) measure place/cancel and
fill/refill cycles at different resting book depths. They include repeatable workload
tests, allocation profiling, and saved before/after measurements for the price-level order-book lesson.

### Measure IPC latency and throughput

Use a dedicated server with a **new, empty Archive directory for each benchmark run**. The workload sends `CancelOrder(1)`; it should receive `cancelled=false` every time. Every warmup and measured request gets a fresh UUID and is recorded, so this measures the request path rather than cached replies. The benchmark fails if a response differs from the expected result.

1. Stop any demo server using the same Aeron directory.
2. Run `AeronEngineServer.main` with program arguments such as `data/latency-run-1 --quiet`, choosing an unused Archive directory. `--quiet` skips per-order result logging while keeping startup messages and errors visible.
3. Run `AeronLatencyBenchmark.main`. Defaults are **500 warmup requests**, **2,000 measured requests**, **one request in flight**, and **sleeping client polling**. Optional program arguments are `warmupCount sampleCount maxInFlight idleMode`, for example `500 2000 8 spin`; warmup may be zero, samples and window size must be positive, and idle mode is `sleep` or `spin`. The window limits outstanding client requests; the engine's log window defaults to eight entries. `sleep` uses `SleepingIdleStrategy`, while `spin` uses `BusySpinIdleStrategy` on the benchmark's polling thread.
4. Stop the benchmark server with **⌃C** when finished. The benchmark closes its client connections and leaves the server running.

The server accepts `--log-window=<positive integer>` to configure how many offered requests can await recording/execution. For example, server arguments `data/latency-run-16 --quiet --log-window=16` pair with benchmark arguments `500 2000 16 sleep`. Startup prints the selected engine window. This bound is independent of the client's outstanding-request limit and the WebSocket gateway's eight active requests. The default remains eight; increasing it permits more queued work but does not add engine threads or change Archive's file/catalog sync levels (both remain 2). The engine executes recorded requests in FIFO order and blocks admission while a reply is unsent.

For an engine-only comparison, hold the client at `32` and test engine windows `8`, `16`, and `32`. Matching both windows at each value instead measures three different concurrency levels. Requests waiting in inbound IPC before engine admission contribute to client latency but are outside server stage timing.

The benchmark fills the selected window using `trySend()`, polls with `pollResponses()`, and refills available slots as replies arrive. It tracks each request by UUID, so replies can arrive out of order. Warmup uses the same window and drains completely before measurement starts. Temporary offer back pressure retains the same encoded request and timer while the loop continues polling replies. Each accepted request gets its own reply deadline and is never resent. Any send failure, reply timeout, or unexpected result aborts the run without printing partial statistics.

The report gives p50, p99, and maximum latency in microseconds (`us`), plus completed requests per second. Percentiles use the nearest-rank convention: p99 is the smallest observed value covering at least 99% of the samples. Each latency timer starts after request construction, before encoding, and ends when its matching reply is handled. It includes time spent retrying unsuccessful offers. Throughput divides measured completions by elapsed time for the entire measured phase, including request construction and UUID generation; it does not divide by the sum of overlapping latencies. Client/stream creation, warmup, sorting, and report printing are excluded from throughput. With zero warmup, waiting for the publication to connect can still affect the first measured requests.

CPU counters are sampled immediately around each phase. The report divides measured-phase CPU time by elapsed time: **100% means one fully occupied core**. `Client thread CPU` covers the thread running the benchmark loop. `Client JVM CPU` includes all threads in that client JVM, including Aeron, JIT, and GC, and can exceed 100%. Neither includes the separate server JVM. Unsupported counters are reported as unavailable. Small differences in sampling boundaries and counter resolution matter more for very short runs. The default sleeping strategy requests only a 1 µs park when idle; it can still use substantial CPU.

Compare windows 1, 2, 4, and 8 with the same idle mode, or compare `500 2000 8 sleep` against `500 2000 8 spin` to change only client polling. Restart the server with a fresh Archive directory for each run. Keep the workload and other machine activity consistent. This is a closed-loop workload: each reply frees capacity for another request, so it measures performance at the selected concurrency rather than under a fixed external arrival rate. It includes codecs, IPC transport, the server's missing-order cancellation path, Archive recording/forced writes, and client polling delays. It does not exercise matching trades or establish maximum throughput. Measurements are not directly comparable to the old buffered-journal baseline. Repeat runs before drawing conclusions; 2,000 samples give only a small view of tail latency.

The [28 September pipeline comparison](benchmarks/results/2026-09-28-aeron-pipeline/report.md) records five runs per window, the throughput/latency tradeoff, raw logs, and environment details.

The [sleeping versus busy-spin comparison](benchmarks/results/2026-09-28-aeron-idle-strategies/report.md) repeats all four windows with both client strategies and measures CPU use. On this Mac and workload, spinning roughly doubled client CPU without a consistent throughput benefit.

The [1 October engine log-window comparison](benchmarks/results/2026-10-01-aeron-log-windows/report.md) compares engine/client windows 8, 16, and 32 across 25 runs. Matching both at 32 delivered roughly four times the throughput of 8/8 with similar median and p99 latency in this cancellation workload. Holding the client at 32 confirms the engine bound matters; raising only client concurrency mostly adds waiting. The default remains eight; the historical gRPC comparison below records the earlier gateway path.

#### Locate time spent inside the server

Add `--stage-timing=500,2000` to the dedicated server's program arguments, alongside `--quiet` and a new Archive directory. Run a single benchmark client with matching warmup/sample counts, then stop the server with **⌃C** to print the stage report. Timing is disabled by default. This diagnostic uses bounded primitive arrays (at most 1,000,000 samples), skips the first configured number of completed fresh logged requests, and excludes cached retries and recovery. The report shows the number actually collected; an interrupted run can be incomplete.

| Stage | Boundaries |
| --- | --- |
| Log offer | Complete decoded request admitted by the agent → successful request-log offer, including back pressure |
| Recording observation | Successful log offer → agent observes the FIFO head's recorded position, including queueing and scheduling |
| Process and size | That observation → command processed and encoded reply size calculated |
| Reply offer | Sized reply ready → successful reply publication, including queueing, encoding, and back pressure |
| Server total | Admission → successful reply publication for the same request |

The server report gives mean, p50, p99, and maximum microseconds. It excludes inbound IPC/decode time before admission and client receipt after publication. **Recording observation is not pure disk-sync time**: Archive polling, forced writes, FIFO delay, and the engine's observation all contribute. Stage means sum to the mean server total (apart from rounding); stage percentiles do not sum to total percentiles. Client and server percentiles also cannot be subtracted to isolate transport time. Samples are written only by the agent and summarized after it stops; no per-request output is added. Compare with timing disabled to check the measurement's impact.

The [server stage comparison](benchmarks/results/2026-09-28-aeron-stage-timing/report.md) contains 20 timing-on/off runs. Recording observation accounted for over 99.8% of mean measured server time at both tested windows (median share across five runs each). The counter measurement below investigates the write-time contribution to that interval.

#### Measure Archive write time

Run the benchmark with `500 2000 1 sleep --archive-counters` against a fresh, dedicated server. Add `--stage-timing=500,2000` to that server to compare its request-stage timings. Repeat with client window `8` to observe batching. The optional fifth benchmark argument reads Aeron's built-in Archive recorder counters; it does not change the server or Archive sync settings.

The report subtracts the post-warmup **total bytes** and **total write time** from the final totals. Write time covers Archive's timed block-write path, including `FileChannel.force` with our sync level 2. `Archive write time / client elapsed` compares aggregate recorder write time with the measured client phase's wall time; it is not a CPU percentage or a per-request latency percentile. Writes may contain several requests. Both maximum-write-time values are **lifetime maxima**, including warmup; their difference is not the measured interval's maximum.

Counter discovery uses numeric type IDs and the Archive ID. Diagnostics require exactly one Archive and no other recording traffic on the driver. The fixed cancel workload has one 64-byte recorded Aeron frame per request. The benchmark checks the expected cumulative byte count before warmup, after warmup, and after measurement, waits for the counter tuple to remain unchanged for 20 ms, and fails on unexpected bytes, missing/replaced counters, or a five-second wait timeout. These checks run outside the latency, throughput, and CPU timers. Archive publishes its counters separately; the settling check reduces boundary races but is not an atomic snapshot or a guarantee against an arbitrarily delayed publication.

The [1 October Archive counter comparison](benchmarks/results/2026-10-01-aeron-archive-counters/report.md) records ten runs. Median aggregate write time was 99.20% of client elapsed time at window 1 and 99.91% at window 8. This points toward the timed write/force path; it does not separate the two calls or measure physical storage latency directly.

### Historical gateway measurements

The [1 October gRPC comparison](benchmarks/results/2026-10-01-grpc-windows/report.md)
preserves the measurements from the retired adapter. Its report identifies the historical
revision and reproduction artifacts. Those results do not measure the current WebSocket
endpoint; a WebSocket latency benchmark is future work.

### Encode commands, requests, and responses with Simple Binary Encoding (SBE)

Run `mvn generate-sources` once, then run `SbeOrderDemo.main` in Zed using the existing `--add-opens` JVM option. It prints the original order, message header, binary bytes in hex, and the decoded order. The final line should be `Equal: true`. This example runs entirely in memory.

Start with these files:

1. `src/main/resources/sbe/orders.xml`: the message schema, defining field types, field order, explicit BID/ASK values, and the header.
2. `src/main/java/dev/sam/exchange/protocol/SbeCommandCodec.java`: adapts `PlaceOrder` and `CancelOrder` to generated buffer encoders/decoders.
3. `src/main/java/dev/sam/exchange/protocol/SbeRequestCodec.java`: encodes and decodes `CommandRequest`, preserving its UUID and command.
4. `src/main/java/dev/sam/exchange/protocol/SbeResponseCodec.java`: encodes and decodes placement, cancellation, and rejection responses, including each placement's trades.
5. `src/main/java/dev/sam/exchange/transport/SbeOrderDemo.java`: the runnable walkthrough.

Maven generates Java codecs under `target/generated-sources/sbe` during `generate-sources`, before compilation and tests. Edit the XML schema and regenerate; generated Java files stay out of Git. The generation execution includes an m2e configuration hint so Zed's Java importer registers that source directory. SBE's generator is a build-plugin dependency, so it does not add a generator dependency to the application's runtime. It is pinned to 1.40.1, which uses our existing Agrona 2.6.0 version.

Every message starts with an 8-byte header. Numbers use little-endian byte order (least significant byte first).

| Message | Template ID | Body bytes | Total bytes |
| --- | --- | --- | --- |
| `PlaceOrder` | 1 | 25 | 33 |
| `CancelOrder` | 2 | 8 | 16 |
| `PlaceOrderRequest` | 3 | 41 | 49 |
| `CancelOrderRequest` | 4 | 24 | 32 |
| `PlaceOrderResponse` | 5 | 36 + 32 × trade count | 44 + 32 × trade count |
| `CancelOrderResponse` | 6 | 25 | 33 |
| `RejectOrderResponse` | 7 | 25 | 33 |

A place command contains an 8-byte order ID, 1-byte side, 8-byte price, and 8-byte quantity. A cancellation contains only the order ID. Request messages prepend the UUID's most-significant and least-significant 64-bit halves to those command fields, adding 16 bytes. Decoding reconstructs the original UUID so later request processing can recognize retries.

A placement response contains 32 bytes of fixed fields (UUID, order ID, and remaining lots), followed by a 4-byte trade-group header and 32 bytes per trade. Even an empty trade list includes the group header. Cancellation and rejection responses contain the UUID, order ID, and a 1-byte cancellation flag or rejection reason.

| Header field | Meaning in this schema |
| --- | --- |
| `blockLength` | Size of the fixed fields for the selected template, excluding the message header and repeating groups. |
| `templateId` | Identifies one of the message layouts above. |
| `schemaId` | Schema identifier: 1 means our exchange schema. |
| `version` | Schema version: currently 0. |

Each adapter reuses its generated encoders and decoders; use each adapter on one thread. Generated codecs wrap the caller's buffer, while decoding creates domain records with independent values. The adapters validate the supplied frame bounds, schema, version, template-specific body size, and enum values. The response decoder also checks the trade-group layout and count, nonnegative remaining lots, and positive trade prices and quantities. `PlaceOrder` retains price/quantity validation. The adapters accept only the exact version-zero layouts; schema evolution will need an explicit compatibility policy.

Tests cover independent binary fixtures, both sides, full-width long and UUID values, nonzero offsets, decoder reuse across buffers, and malformed messages. Live IPC requests and the server-owned persistent request log use `SbeRequestCodec`; live responses use `SbeResponseCodec`. Transport tests preserve byte-identical retries, force request fragmentation with a small test-only MTU, and verify reassembly of replies containing many trades. Malformed replies cannot complete a pending request.

Reference: [SBE Java users guide](https://github.com/aeron-io/simple-binary-encoding/wiki/Java-Users-Guide).

### Record and replay SBE requests with Aeron Archive

Run `AeronArchiveDemo.main` in Zed with the same `--add-opens` JVM option shown above. It starts its own embedded Archive and Media Driver with a unique driver directory. No separate server or client is needed.

The demo:

1. Creates a temporary directory for Archive's catalog and recording files.
2. Wraps a ten-lot bid at 100 and a four-lot ask at 99 in requests with UUIDs, encodes them using `SbeRequestCodec`, and publishes the binary messages on recorded IPC stream **1001**.
3. Waits until Archive's recording position reaches the publication's final position, then stops the recording.
4. Closes Archive and the driver, then starts new instances using the saved archive directory.
5. Replays the saved recording onto IPC stream **1002** and decodes the messages into an immutable list of requests.
6. Checks that the UUIDs and commands match the original requests, then uses `ReplayRunner` to apply the recovered commands to a fresh matching engine.

The output should include **`Requests preserved: true`**, a **four-lot trade at 100**, and a recovered book containing **six remaining bid lots**. The archive directory stays on disk after the demo; each main run creates a new directory. Driver shared-memory files are removed on shutdown.

Read the class in this order: `main`, `record`, `replay`, then the setup/wait helpers. The new types are:

| Type | Role in the demo |
| --- | --- |
| `ArchivingMediaDriver` | Owns an embedded Media Driver and Archive service. |
| `AeronArchive` | Control client used to request recording, stopping, and replay. |
| `RecordingPos` | Finds a recording's ID and progress counter for the publication session. |
| `Image` | The particular replay session being consumed; its position tells us when replay finishes. |

A **recording ID** identifies saved data in the archive catalog. The **recording subscription ID** returned by `startRecording` identifies the subscription to stop. A **position** counts framed stream bytes, not orders. Keep application streams distinct from Archive's control streams, which default to 10 and 20.

Archive records the published bytes. The replay handler passes the fragment assembler's buffer, offset, and length directly to `SbeRequestCodec`; it recovers requests without consulting the original request list. Integration tests verify UUID and request order preservation, trade prices, remaining quantities, empty recordings, repeated requests across multiple polling batches, and selecting recordings across Archive restarts.

This standalone lesson preserves repeated requests exactly as recorded, then `ReplayRunner` applies every command. The live server uses `RequestStateMachine` during recovery to rebuild deduplication as well as the book. The demo explicitly uses file/catalog sync level zero: it demonstrates normal-restart recovery, not power-loss durability. Its waits have five-second deadlines and use sleeping idling; it is a small learning demo, not a latency benchmark.

Reference: [Aeron Archive overview](https://aeron.io/docs/aeron-archive/overview/).

### Tests and layout

Integration tests launch real JVMs and use isolated temporary Archive and Aeron directories. They cover book and cached-reply recovery across server restarts, retries across client reconnects, automatic client retry limits and delayed duplicate replies, UUID conflicts followed by valid commands, abrupt process death, stopped recordings, shutdown, fragmented requests and trade replies, and ignoring unrelated or stale replies.

Code lives under `src/main/java/dev/sam/exchange`:

- `engine`: order book, matching, commands, results, snapshots, and replay.
- `persistence`: `ArchiveRuntime`, `ArchiveRequestLog`, and the narrow `RequestLog` interface used by the agent.
- `protocol`: the text command codec and SBE adapters, with generated message codecs in `protocol.sbe` under the build output directory.
- `transport`: Aeron demos, reusable `AeronRequestClient`, request/response codecs, request deduplication, and request recovery.
- `gateway`: bounded `EngineGateway`, strict JSON/domain mapping, Netty WebSocket handling, and the standalone gateway launcher. The browser contract is documented in `docs/websocket-contract.md`.

`AeronEngineClient.main` creates and closes the Aeron connections, assigns request UUIDs, and prints results. `AeronRequestClient` borrows the publication and subscription and handles encoding, retries, and correlated replies. Use each client instance from one thread, with one request at a time.

`ClientConfig` sets the timeout for each send and reply wait and the maximum number of attempts. Defaults remain five seconds and three attempts. Configuration requires a non-null positive duration and at least one attempt.

Tests mirror these packages under `src/test/java`. Zed and Spotless share Eclipse formatting preferences, and Zed formats Java on save.

## Current demo limits

- The client grows its request buffer, the server grows its reply buffer, and both receivers reassemble fragmented messages. Messages must still fit within Aeron's maximum message length.
- The client waits for one request at a time. Each send and reply wait has a five-second deadline by default; send failures still stop the client. Client request IDs are held in memory, so automatic retries apply within the current client run. Server reply-send failures time out after five seconds and currently stop the server.
- Archive persistence is local to this machine. Tests cover clean restart, abrupt process death, and recorded-but-unapplied recovery, not a physical power-cut or failing disk.
- The request cache and recording grow without a retention limit. Startup streams the full recording; snapshots and retention are still to come.

## What I want to explore next

1. Refine client sessions, reply delivery, and service lifecycle behavior.
2. Measure throughput and tail latency, then study allocation, GC, data layout, and JVM behavior.
3. Add persisted snapshots and safe log/cache retention; define protocol schema evolution.
4. Introduce Aeron Cluster, replicated execution, persisted snapshots, and failover.

The idea is to build understanding incrementally and let measurements guide the performance work.
