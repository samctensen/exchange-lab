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
- Request journaling and deterministic replay for normal server restarts.
- Separate Aeron IPC client and server processes, with UUIDs connecting requests to replies.
- Request deduplication across client reconnects and normal server restarts.
- Bounded client retries after reply timeouts, reusing the same UUID and command.
- A server that stays available between client sessions and closes its resources on a shutdown request.

This is a learning implementation. Best-price selection still scans the book, and the server processes commands on one thread.

## How a request moves through the system

```text
Client: CommandRequest(UUID, EngineCommand)
  -> Aeron IPC stream 1
  -> Server: return a cached response, or journal, validate, and process a new request
  -> CommandResponse(same UUID, CommandResult)
  -> Aeron IPC stream 2
  -> Client: accept the reply whose UUID matches its current request
```

A duplicate order ID that is still on the book produces a `RejectResult` without changing the book. First-time requests are journaled even when rejected, so replay can reconstruct the same response. Cancelling a missing order returns `CancelResult` with `cancelled=false`.

The server remembers each completed request's UUID, command, and response. Retrying the same UUID and command returns the original response without changing the journal or order book. Reusing a UUID with a different command returns `REQUEST_ID_CONFLICT` and preserves the original cached entry. Journal-write failures propagate without caching a response.

After a reply timeout, the client resends the same request, up to three total attempts by default. A delayed matching reply completes the request; duplicate replies for an earlier order are ignored. If all attempts go unanswered, the client stops before sending the next order and reports the request UUID with an unknown outcome, since the server may already have processed it. A send failure on a later attempt also preserves the request UUID and unknown-outcome diagnostic.

The journal stores request UUIDs together with engine commands. Recovery applies those requests in order to rebuild both the book and cached responses, without appending them again. Repeated UUIDs in the journal are rejected as corruption; live retries and UUID conflicts never append another entry.

## Run locally

Install JDK 25 and Maven 3.9+, and make sure `mvn -v` reports Java 25.

```sh
mvn test             # Run the tests
mvn verify           # Build, test, and check formatting
mvn spotless:apply   # Format Java sources
```

### Run the IPC demo in Zed

1. Run `AeronEngineServer.main` and leave it running.
2. Run `AeronEngineClient.main` in another process. It sends a ten-lot bid and a four-lot ask, then prints their results.
3. Stop the server with **⌃C** in its terminal when finished. Its shutdown hook requests that the processing loop stop and waits for resource cleanup.

The server defaults to `data/requests.journal`, which is ignored by Git. Its first program argument can select another journal path; missing parent directories and a new journal are created at startup. An existing request journal is replayed before requests are accepted. Older command-only journals do not contain request UUIDs and cannot be used by this server; start with a new request journal path.

The sample client uses fixed order IDs. Running it again against the same book can therefore produce a duplicate-ID rejection. A clean demo run can use a new journal path.

Both processes must use the same `java.io.tmpdir`, where the server creates `exchange-lab-aeron`. Aeron requires this JVM option:

```text
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
```

The project configures it for Maven tests and Zed terminals. Include it in the launch configuration when running elsewhere. Use matching client/server versions: the IPC protocol now expects `REQUEST,<uuid>,<command>` and `RESPONSE,<uuid>,<result>` wrappers.

`AeronEngineServer.main` owns recovery, Aeron resources, and shutdown. An Agrona `AgentRunner` calls `AeronEngineAgent.doWork()` and handles idling on the dedicated `exchange-engine` thread. Each pass assembles requests, processes a complete command, and attempts to publish its response once. An unsent reply keeps its encoded bytes and original deadline between passes; the agent waits to process another request until that reply is queued. Main waits for shutdown or runner termination, then closes the runner before its Aeron connections. Agent failures stop the worker and are rethrown by main after cleanup, preserving their original cause. Journal writes remain synchronous on the agent thread. Tests cover shutdown while idle or while a reply has no subscriber, plus cleanup and failed process exits after journal-write errors and reply timeouts.

The engine runner uses `BusySpinIdleStrategy`, following [Aeron’s guidance for low-latency subscribers](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide#application-threads). It keeps polling when idle and can consume roughly one CPU core. Budget a dedicated core for this approach in production; this demo leaves CPU placement to the operating system and does not reserve or pin a core. Stop the server with **⌃C** when finished.

### Measure IPC round-trip latency

Use a dedicated server with a **new, empty journal for each benchmark run**. The workload sends `CancelOrder(1)`; it should receive `cancelled=false` every time. Every warmup and measured request gets a fresh UUID and is journaled, so this measures the request path rather than cached replies. The benchmark fails if a response differs from the expected result.

1. Stop any demo server using the same Aeron directory.
2. Run `AeronEngineServer.main` with program arguments such as `data/latency-run-1.journal --quiet`, choosing an unused journal path. `--quiet` skips per-order result logging while keeping startup messages and errors visible.
3. Run `AeronLatencyBenchmark.main`. Defaults are **500 warmup requests** followed by **2,000 measured requests**. Optional program arguments are `warmupCount sampleCount`, for example `100 1000`; warmup may be zero and samples must be positive.
4. Stop the benchmark server with **⌃C** when finished. The benchmark closes its client connections and leaves the server running.

The report gives p50, p99, and maximum latency in microseconds (`us`). Percentiles use the nearest-rank convention: p99 is the smallest observed value covering at least 99% of the samples. Each timer starts after request construction and ends when `client.send()` returns its matching response. Warmup, UUID generation, sorting, and report printing are outside the measured intervals. A send or reply failure aborts the run without printing partial statistics; requests get one attempt each.

This is a baseline with one request outstanding at a time. It includes codecs, IPC transport, the server's missing-order cancellation path, journal writes, and idle-strategy delays. It does not exercise matching trades or measure maximum throughput. Journal writes still are not forced to disk, so these numbers do not represent power-loss durability. Repeat runs with fresh journals before drawing conclusions; 2,000 samples give only a small view of tail latency.

### Encode commands and requests with Simple Binary Encoding (SBE)

Run `mvn generate-sources` once, then run `SbeOrderDemo.main` in Zed using the existing `--add-opens` JVM option. It prints the original order, message header, binary bytes in hex, and the decoded order. The final line should be `Equal: true`. This example runs entirely in memory.

Start with these files:

1. `src/main/resources/sbe/orders.xml`: the message schema, defining field types, field order, explicit BID/ASK values, and the header.
2. `src/main/java/dev/sam/exchange/protocol/SbeCommandCodec.java`: adapts `PlaceOrder` and `CancelOrder` to generated buffer encoders/decoders.
3. `src/main/java/dev/sam/exchange/protocol/SbeRequestCodec.java`: encodes and decodes `CommandRequest`, preserving its UUID and command.
4. `src/main/java/dev/sam/exchange/transport/SbeOrderDemo.java`: the runnable walkthrough.

Maven generates Java codecs under `target/generated-sources/sbe` during `generate-sources`, before compilation and tests. Edit the XML schema and regenerate; generated Java files stay out of Git. The generation execution includes an m2e configuration hint so Zed's Java importer registers that source directory. SBE's generator is a build-plugin dependency, so it does not add a generator dependency to the application's runtime. It is pinned to 1.40.1, which uses our existing Agrona 2.6.0 version.

Every message starts with an 8-byte header. Numbers use little-endian byte order (least significant byte first).

| Message | Template ID | Body bytes | Total bytes |
| --- | --- | --- | --- |
| `PlaceOrder` | 1 | 25 | 33 |
| `CancelOrder` | 2 | 8 | 16 |
| `PlaceOrderRequest` | 3 | 41 | 49 |
| `CancelOrderRequest` | 4 | 24 | 32 |

A place command contains an 8-byte order ID, 1-byte side, 8-byte price, and 8-byte quantity. A cancellation contains only the order ID. Request messages prepend the UUID's most-significant and least-significant 64-bit halves to those command fields, adding 16 bytes. Decoding reconstructs the original UUID so later request processing can recognize retries.

| Header field | Meaning in this schema |
| --- | --- |
| `blockLength` | Fixed body size for the selected template, excluding the header. |
| `templateId` | Identifies one of the four message layouts above. |
| `schemaId` | Schema identifier: 1 means our exchange schema. |
| `version` | Schema version: currently 0. |

Each adapter reuses its generated encoders and decoders; use each adapter on one thread. Generated codecs wrap the caller's buffer, while decoding creates domain records with independent values. The adapters validate the supplied frame bounds, schema, version, template-specific body size, and side. `PlaceOrder` retains price/quantity validation. They accept only the exact version-zero layouts; schema evolution will need an explicit compatibility policy.

Tests cover independent binary fixtures, both sides, full-width long and UUID values, nonzero offsets, decoder reuse across buffers, and malformed messages. The live IPC server still uses its text request/response codecs and `RequestJournal`.

Reference: [SBE Java users guide](https://github.com/aeron-io/simple-binary-encoding/wiki/Java-Users-Guide).

### Tests and layout

Integration tests launch real JVMs and use isolated temporary journals and Aeron directories. They cover book and cached-reply recovery across server restarts, retries across client reconnects, automatic client retry limits and delayed duplicate replies, UUID conflicts followed by valid commands, shutdown, fragmented requests and trade replies, and ignoring unrelated or stale replies.

Code lives under `src/main/java/dev/sam/exchange`:

- `engine`: order book, matching, commands, results, snapshots, and replay.
- `persistence`: text command codec, the original command journal, and the request journal used by the server.
- `protocol`: SBE adapters, with generated message codecs in `protocol.sbe` under the build output directory.
- `transport`: Aeron demos, reusable `AeronRequestClient`, request/response codecs, request deduplication, and request recovery.
- `JournaledEngine`: the earlier command-only validation, journaling, processing, and recovery lesson.

`AeronEngineClient.main` creates and closes the Aeron connections, assigns request UUIDs, and prints results. `AeronRequestClient` borrows the publication and subscription and handles encoding, retries, and correlated replies. Use each client instance from one thread, with one request at a time.

`ClientConfig` sets the timeout for each send and reply wait and the maximum number of attempts. Defaults remain five seconds and three attempts. Configuration requires a non-null positive duration and at least one attempt.

Tests mirror these packages under `src/test/java`. Zed and Spotless share Eclipse formatting preferences, and Zed formats Java on save.

## Current demo limits

- The client grows its request buffer, the server grows its reply buffer, and both receivers reassemble fragmented messages. Messages must still fit within Aeron's maximum message length.
- The client waits for one request at a time. Each send and reply wait has a five-second deadline by default; send failures still stop the client. Client request IDs are held in memory, so automatic retries apply within the current client run. Server reply-send failures time out after five seconds and currently stop the server.
- Journal writes are not explicitly forced to disk. Recovery tests cover normal restarts; an incomplete final journal line is rejected.
- The request cache and journal grow without a retention limit. Startup reads and replays the whole journal.

## What I want to explore next

1. Refine client sessions, reply delivery, and service lifecycle behavior.
2. Measure throughput and tail latency, then study allocation, GC, data layout, and JVM behavior.
3. Introduce Aeron Cluster, replicated execution, persisted snapshots, and failover.

The idea is to build understanding incrementally and let measurements guide the performance work.
