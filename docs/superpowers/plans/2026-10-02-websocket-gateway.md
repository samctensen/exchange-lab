# WebSocket Gateway Implementation Plan

> Execution: implement inline using the executing-plans and test-driven-development skills; obtain a fresh final code review.

**Goal:** Submit browser-compatible orders over WebSocket through the existing recorded engine path.

**Architecture:** A JSON codec and Netty adapter feed EngineGateway. The adapter owns sockets; EngineGateway retains Aeron request scheduling and correlation. A standalone main owns startup and shutdown.

**Tech Stack:** Java 25, Netty 4.2.18.Final, Gson 2.14.0, existing Aeron 1.53.0/SBE, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-02-websocket-gateway-design.md`

## Global constraints

- Loopback only, default port 8080, path `/orders`; no production authentication claims.
- 16 KiB inbound messages, 32 outstanding requests per connection, 128 connections, 64/128 KiB write watermarks, 1 MiB maximum response.
- Preserve engine request IDs, deduplication, and accepted-command behavior after disconnect.
- Keep source edits visible in the user's checkout. Run Maven in a temporary source copy to avoid Zed/JDT output races. The user requested focused commits and a PR after implementation, including retirement of the gRPC gateway.

## Review focus

1. Values above JavaScript's safe integer limit must survive unchanged (codec tests).
2. Failed or disconnected sockets must not block/cancel the engine worker (handler and real integration tests).
3. Concurrent connections must not receive another connection's result (out-of-order routing tests).
4. Fragmentation or invalid frames must never bypass size/admission limits (socket/handler tests).
5. Bind failure and shutdown must release event loops and socket resources (lifecycle tests).

## Task 1: JSON contract and queue admission errors

Files: `WebSocketCommandCodec.java`, `GatewayOverloadedException.java`, `EngineGateway.java`, corresponding tests, `pom.xml`.

Interface: `CommandRequest decode(String text)`, `String encode(UUID id, CommandResult result)`, `String error(UUID id, String code, String message, boolean outcomeUnknown)`.

- [x] Write codec tests with literal JSON and domain expectations, including `"9223372036854775807"`, duplicate fields, and business rejection.
- [x] Run the focused tests and observe missing behavior.
- [x] Implement strict decoding, response encoding, and a typed queue-full rejection.
- [x] Re-run focused codec/admission tests to green.

## Task 2: Bounded asynchronous WebSocket adapter

Files: `WebSocketGateway.java`, `WebSocketGatewayTest.java`, `WebSocketOrderHandlerTest.java`.

Interface: `WebSocketGateway(EngineGateway gateway, int port)`, `start()`, `port()`, `awaitClose()`, `close()`; server does not own the supplied EngineGateway.

- [x] Add real Java HttpClient WebSocket tests for place/trade/cancel, independent connections, fragmented messages, bad input, reconnect retry, bind failure, and shutdown. Add embedded-channel tests for pending limits and unwritable channels.
- [x] Run tests and observe absent adapter behavior.
- [x] Implement Netty upgrade/frame pipeline and event-loop confined completion routing. Close on protocol/size violations, and bound pending work and output.
- [x] Run focused tests to green.

## Task 3: Runnable service, recorded engine test, and documentation

Files: `WebSocketGatewayServer.java`, `WebSocketGatewayIntegrationTest.java`, `WebSocketGatewayServerTest.java`, `docs/websocket-contract.md`, `README.md`, `docs/architecture.md`.

- [x] Add failing CLI validation and real Aeron/Archive integration tests. The integration must prove retry after reconnect does not match twice.
- [x] Implement CLI and lifecycle, then run focused tests.
- [x] Document literal browser messages, startup, error/retry semantics, and lab limitations.
- [x] Apply formatting; run `mvn clean verify` in the isolated verification copy.
- [x] Obtain final review; fix concrete findings with regression tests. Report exact validation and next lesson.

## Progress

- Context: clean main at ff7b85b; implementation authorized by the user's “okay lets cook it up” following the proposed first-gateway scope.
- Decision: independent small JSON contract reuses domain commands without adding a dependency on the gRPC schema. A production public API will need authentication and account identity first.

## Verification and review record

- Task 1: codec tests first failed because the codec did not exist; codec and typed-admission tests passed after implementation.
- Task 2: handler and socket tests first failed because the adapter did not exist. The real oversized-fragment test then exposed a normal-close code instead of 1009. Netty's aggregator raises TooLongFrameException; explicitly mapping that exception to 1009 made the regression pass.
- Task 3: CLI tests first failed because the entry point did not exist; CLI and real recorded-engine restart tests passed after implementation.
- Verification before gRPC retirement: `mvn clean verify` in `/private/tmp/exchange-websocket-verify`: **608 tests, 0 failures/errors/skips; Spotless clean; BUILD SUCCESS**. The temporary source copy avoids racing Zed/JDT output in the checkout.
- Process smoke check: actual AeronEngineServer and WebSocketGatewayServer mains in separate JVMs with temporary storage, a real WebSocket cancel response, and both processes completing SIGTERM shutdown.
- Fresh read-only review: no Critical or Important findings. The reviewer approved the implementation conditional on the now-passing full suite.
- Final: minor (deferred): add a real-socket disconnect after admission but before completion; current pending-disconnect coverage uses an embedded Netty channel, while the real Archive test covers retry after receiving a response and restarting.
- Final: minor (deferred): directly assert event-loop termination after failed bind; current test checks failed startup cleanup and that the existing listener still serves requests.
- Final: Ruling: production authentication/WSS, ownership/risk controls, and replication stay outside this local lesson. Public deployment remains inappropriate until those controls exist.
- Final: Ruling: cross-process gateway isolation stays outside this slice. Run one gateway launcher at a time until scoped identities and reply routing are implemented.
- Final: Ruling: transport performance comparison stays outside this slice. No latency advantage over gRPC is claimed or measured.
- The user subsequently requested commits and a PR on `codex/websocket-trading-gateway`, and confirmed that the entire gRPC gateway is no longer needed. No merge was requested.

## Task 4: Retire gRPC during PR preparation

- [x] Remove the protobuf schema, generator, runtime dependencies, gRPC adapter/client/benchmark launcher, and protocol-specific tests.
- [x] Port the real-Aeron concurrency test to two WebSocket clients, preserving the eight-request window and reverse-order reply checks.
- [x] Update current architecture/run documentation and retain the old benchmark results with an explicit historical revision.
- [x] Verify a clean build has no generated protobuf classes or gRPC dependencies; run the full suite and review the retirement diff.

### Retirement verification

- `mvn clean verify dependency:tree`: **538 tests, 0 failures/errors/skips; Spotless clean; BUILD SUCCESS**. Seventy gRPC-specific test cases were retired; the real-Aeron pipeline test was migrated to WebSockets.
- The resolved dependency tree contains no gRPC/protobuf libraries. The clean build produces no protobuf source tree, packaged `.proto` resources, or gRPC/protobuf classes.
- The actual engine and WebSocket launcher passed a separate-process submission and shutdown smoke test after removal.
- Fresh read-only review of the retirement found no issues. Compatibility with old gRPC clients is intentionally removed; historical benchmark reproduction uses the documented former revision.
- Delivery: publish the reviewed branch in focused commits and open one PR, as requested. The GitHub PR records publication status.
