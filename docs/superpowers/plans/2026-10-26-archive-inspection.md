# Archive Inspection Implementation Plan

> **For agentic workers:** Use test-first implementation in isolated domains, integrate reviewed checkpoints sequentially, and request a fresh whole-branch review before publishing.

**Goal:** Inspect a stopped exchange Archive, reconstruct its book, summarize outcomes and compare histories without submitting new commands.

**Architecture:** Reuse ArchiveRequestLog.replay and the production RequestStateMachine. Add query-only order-book projections and an audit consumer. A dedicated CLI owns an isolated temporary Media Driver and Archive runtime, renders only a completed replay, and exits. Archive startup may update its own housekeeping metadata; this is not a byte-for-byte read-only filesystem reader.

**Tech Stack:** Existing Java 25, Maven, JUnit, Aeron 1.53 and Gson; no new dependencies or wire/schema changes.

**Spec:** Sam's October 26 request delegates implementation choices and authorizes 15–20 useful signed commits, a PR and automatic merge after checks. The selected feature and interfaces are coordinator decisions.

## Constraints and review focus

- Do not change matching, UUID deduplication, Archive sync settings or live gateway behavior.
- Queries execute on the owning thread; returned projections are immutable. Aggregated lots and trade notional use BigInteger to avoid signed-long overflow.
- Repeated UUID/identical commands count as duplicate observations but do not double-count executions or trades. UUID conflicts remain distinct rejections.
- Inspect only an existing Archive directory with its catalog. Never create a fresh history for a typo. Do not run beside a live owner; preserve Archive's ownership checks.
- Default CLI depth is ten levels per side; accepted range is 1..1000. Text and JSON reports contain aggregate data, no UUIDs or individual order IDs.
- Fingerprints identify the ordered request history; they are not a signature, corruption repair, or proof that two final books differ.
- Errors and interruption produce no success report. Replay failure can leave partial in-memory state, which is discarded.
- Real Archive/subprocess tests use isolated temporary directories. Parent runs full build and benchmarks once after integration, then cross-platform CI.

## Interfaces and checkpoints

### A: order-book projections (engine package)

Own OrderBook.java, PriceLevel.java and new PriceLevelSnapshot.java, BookDepth.java, BookSummary.java; focused tests only.

- [x] A1: `PriceLevelSnapshot(long priceTicks, int orderCount, BigInteger quantityLots)` and `PriceLevel.snapshot()`; validate nonnegative empty-level counts/volume consistently and positive price; sum current remaining lots exactly.
- [x] A2: `BookDepth(List<PriceLevelSnapshot> bids, List<PriceLevelSnapshot> asks)` and `OrderBook.depth(int maxLevels)`; immutable lists, bid descending/ask ascending, zero empty, negative rejected, bounded per side, no mutation.
- [x] A3: `BookSummary(int bidOrders, int askOrders, int bidLevels, int askLevels, BigInteger bidLots, BigInteger askLots)` and `OrderBook.summary()`; full-book aggregates independent of depth truncation.
- [x] A4: Independently modeled mixed add/fill/cancel regression sequence checks depth, full totals and FIFO preservation; avoid implementation-mirroring tests.

### B: replay accounting (new audit package)

Own ReplayAudit.java, ReplayAuditSnapshot.java, RequestHistoryDigest.java and focused tests. No changes to engine/persistence/transport existing classes.

- [x] B1: `ReplayAudit(RequestStateMachine state)` implements `Consumer<CommandRequest>` with `snapshot()`; counts observed, unique, placements, cancellations and business rejections for a fresh state machine.
- [x] B2: Classify cached duplicate and conflicting UUID observations without counting them as fresh executions.
- [x] B3: Count trades, executed lots and tick-lot notional only for unique placement results; use exact BigInteger volume/notional.
- [x] B4: `RequestHistoryDigest` maintains a versioned SHA-256 chain over exact SBE request frames; repeated observations, order, UUID and payload affect it. Snapshot digest is nondestructive and incremental.
- [x] B5: Failure/interruption contracts and immutable observation snapshots; state/digest/counters must not silently continue after a failed audit callback. Pin with meaningful failures through real RequestStateMachine subclass boundary if necessary.

`ReplayAuditSnapshot` fields: long observedRequests, uniqueRequests, duplicateRequests, conflictingRequests, placeRequests, cancelRequests, businessRejections, trades; BigInteger executedLots, notionalTickLots; String historySha256. Counters describe unique executions unless explicitly named observations. RequestHistoryDigest algorithm agreed in implementation notes before integration.

### C: inspector integration (coordinator)

Own new audit/ArchiveInspectorConfig.java, ArchiveInspection.java, ArchiveInspectionReport.java, ArchiveInspector.java, integration tests and docs.

- [x] C1: Strict `--archive=path`, `--levels=n`, `--format=text|json`, standalone `--help`; validate before resources.
- [x] C2: Human-readable completed inspection report using audit snapshot, full summary and bounded depth.
- [x] C3: Versioned JSON report with exact decimal strings for long/BigInteger values and deterministic field order.
- [x] C4: Replay existing stopped Archive using an isolated owned temporary driver; no live ingress or request-log writer. Return aggregate report after replay succeeds.
- [x] C5: Validate existing catalog and protect runtime ownership/lifecycle; failed startup or replay leaves no success output and cleans up owned driver files.
- [x] C6: End-to-end CLI fixtures verify exact reports, unchanged recording stop position and continued normal server recovery.
- [x] C7: CLI help, examples, fingerprint interpretation and operational constraints; complete plan evidence.

Checkpoint boundaries may be combined or split based on real implementation seams, targeting 15–20 meaningful commits without padding.

## Integration evidence

- Engine projections passed independent mixed-operation ledgers across three fixed seeds,
  including full-long quantities, partial fills, cancellations and FIFO checks.
- Replay audit passed independent SBE byte/hash fixtures, exact notional totals, cached
  result accounting and permanent failure/interruption handling.
- Real Archive and child-JVM fixtures verified text/JSON reports, bounded depth,
  unchanged recording stop positions, recovery/extension after inspection, active-owner
  refusal, malformed-frame failure without partial reports and temporary runtime cleanup.
- C6 was split into the CLI implementation and a separate real-process failure-regression
  commit. The completed feature has 17 commits including operational documentation.

## Release gates

These gates govern publication; their final outcomes are recorded in the PR and task response.

- Fresh whole-branch review and resolution of actionable findings.
- Clean install, complete test suite and formatting.
- Benchmark module clean verify.
- Verify signatures, clean checkout and tested-input equality.
- Push, attach PR, observe both CI jobs and merge while preserving individual commits. GitHub auto-merge is disabled for this repository; the coordinator waits for passing checks and merges directly.
- Fast-forward local main and verify merged tree.
