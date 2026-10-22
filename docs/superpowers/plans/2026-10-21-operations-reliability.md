# Operations and Recovery Reliability Implementation Plan

> **For agentic workers:** Use test-first implementation in three isolated domains, integrate reviewed checkpoints sequentially, and perform one fresh whole-branch review.

**Goal:** Make the local gateway configurable and observable while making Archive recovery interruption and failure handling predictable.

**Architecture:** Keep Java 25, Netty, Aeron IPC/SBE and the single ordered Archive recording. Add an immutable socket-limit policy, optional Prometheus text metrics from existing snapshots, and narrowly scoped recovery fixes. Defaults and existing wire contracts remain compatible.

**Tech Stack:** Java 25, Maven, JUnit 5, Netty 4.2, Aeron 1.53; no new dependencies.

**Spec:** The user's October 21 request authorizes an autonomous improvement batch of about 30 signed commits, followed by a PR and merge after checks. Scope and implementation choices are recorded below.

## Global Constraints

- Keep the gateway bound to loopback; authentication and TLS are outside this batch.
- Preserve existing constructors and default limits: 128 connections, 32 pending requests per socket, 16 KiB inbound messages, 1 MiB responses, 60-second read idle and 5-second handshake timeouts.
- Preserve UUID retry semantics, recording-before-execution and the borrowed-resource ownership model.
- Metrics are disabled by default, expose aggregates only and never publish or consume engine messages.
- Each production behavior gets a failing regression or contract test before implementation. Human documentation gets a manual accuracy/link check.
- Each checkpoint includes relevant tests. The final combined build runs once after integration, then CI runs on Ubuntu and macOS.
- Only the coordinator modifies Git state; agent patches are developed in separate scratch directories.

## Interfaces

- `WebSocketLimits(int maxConnections, int maxPendingPerConnection, int maxMessageBytes, int maxResponseBytes, long readIdleTimeoutMillis, long handshakeTimeoutMillis)` validates positive values and timeouts representable in nanoseconds; `defaults()` preserves current behavior.
- `WebSocketGateway(EngineGateway, int, WebSocketLimits)` keeps the existing two-argument constructor.
- `GatewayMetrics.render(GatewayDiagnostics.Snapshot, boolean ready)` emits fixed Prometheus metric names, HELP/TYPE metadata and LF-terminated numeric samples.
- `GatewayHttpHandler(BooleanSupplier ready, Supplier<String> metrics)` uses a nullable metrics supplier to disable `/metrics`; the existing constructor remains supported.
- Launcher flags configure socket limits independently of the shared engine queue and retry timeouts. `--metrics` enables collection and HTTP export; only `--diagnostics` requests the shutdown report.

## Review Focus

- Invalid or overflowing flags fail before opening Aeron, allocating event loops or listening.
- Slow clients, fragmented requests and completion after disconnect do not bypass configured limits or cancel accepted commands.
- HEAD and rejected metrics requests preserve correct lengths, close connections and avoid unintended state changes.
- Catalogue lookalikes cannot select the wrong authoritative request recording; exact duplicate recordings still fail recovery.
- Interruption and callback/decode failures release owned resources without acknowledging unrecorded work or obscuring the primary failure.

## Checkpoint Plan

For each code checkpoint: write the behavior test, observe its expected failure, implement the smallest change, run the focused tests and formatting, review the patch, then sign its commit.

### Socket limits (L1–L7)

Files: `gateway/WebSocketLimits.java`, `gateway/WebSocketGateway.java`, `gateway/WebSocketOrderHandler.java`, focused gateway tests.

- [x] L1: Validated immutable limits and compatible gateway construction.
- [x] L2: Connection budget and slot reuse after close.
- [x] L3: Per-socket pending budget and completion release.
- [x] L4: Configured inbound frame, aggregate and HTTP body limits.
- [x] L5: Configured outbound response limit with unknown-outcome close semantics.
- [x] L6: Configured read-idle timeout.
- [x] L7: Configured WebSocket handshake timeout.

### Metrics (M1–M7)

Files: `gateway/GatewayMetrics.java`, `gateway/GatewayHttpHandler.java`, focused renderer and handler tests.

- [x] M1: Admission counters in Prometheus text format.
- [x] M2: Completion, business rejection, failure and unmatched-response counters.
- [x] M3: Offer, retry and timeout counters.
- [x] M4: Availability, high-water and queue-wait measurements with explicit units.
- [x] M5: Opt-in GET `/metrics` with bounded aggregate content.
- [x] M6: HEAD, method/body/Origin validation and exact routing.
- [x] M7: Controlled export failure without leaking internal errors or breaking liveness.

### Recovery (R1–R7)

Files: `persistence/ArchiveRequestLog.java` and focused persistence tests.

- [x] R1: Exact catalogue alias matching with pagination.
- [x] R2: Interrupted replay fails before catalogue access or callbacks.
- [x] R3: Interrupted open does not create a writer.
- [x] R4: Interruption during callbacks stops before the next command.
- [x] R5: Callback failures release replay resources and preserve retryability.
- [x] R6: Decode errors report recording context while preserving the cause.
- [x] R7: Failed close attempts recording cleanup while preserving uncertainty and failures.

### Launcher integration and guides (P1–P9)

Files: `gateway/WebSocketGatewayServer.java`, focused launcher integration tests, `gateway/WebSocketGateway.java` metrics wiring, README and operating guides.

- [x] P1: `--max-connections` config and real launcher enforcement.
- [x] P2: `--max-pending-per-connection` config and admission enforcement.
- [x] P3: `--max-message-bytes` config and frame enforcement.
- [x] P4: `--max-response-bytes` config and result enforcement.
- [x] P5: `--read-idle-timeout-ms` config and idle connection enforcement.
- [x] P6: `--handshake-timeout-ms` config and handshake enforcement.
- [x] P7: `--metrics` wiring and real read-only scrape tests, independent of shutdown diagnostics.
- [x] P8: Socket-limit guide, CLI reference and defaults.
- [x] P9: Metrics and recovery operating guidance, examples and limitations.

## Verification and Integration

- [x] Independent whole-branch review; no actionable findings.
- [x] `mvn --batch-mode --no-transfer-progress clean install`: 1,153 tests, zero failures/errors/skips; packaging and formatting passed.
- [x] `mvn --batch-mode --no-transfer-progress -f benchmarks/pom.xml clean verify`: passed.
- [ ] Check documentation links, tested input equality, signatures and clean working tree.
- [ ] Push branch, create and attach PR, wait for both CI jobs, merge preserving commits.
- [ ] Update local main and verify the merged state.

Checkpoint boundaries may change if a test proves that a proposed fix is already covered or two changes cannot stand alone; record those decisions rather than adding filler commits.

## Review rulings

- Keep fixed output watermarks and post-encoding response-size validation; both are explicit limits in the resource guide.
- Failed replay can leave partial in-memory state; retry uses a fresh state machine, while the saved recording remains authoritative.
- Readiness is sampled separately from counters. HTTP management routes share connection capacity. Neither promises admission or delivery.
- Authentication, TLS, replication, retention and stronger storage guarantees remain outside this batch.
- Fresh review covered the completed code and guides; no actionable findings remained.
