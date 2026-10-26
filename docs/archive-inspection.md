# Inspect a stopped exchange Archive

`ArchiveInspector` replays the saved request history into a fresh matching engine
and prints aggregate outcomes and order-book depth. It does not open a request-log
writer or append requests. It uses the same recording selection, SBE decoder,
matching rules and UUID deduplication as engine recovery.

Stop the engine and any other owner of the Archive directory first. Preserve a
backup, or inspect a copy made while the owner is stopped. The command starts a
private Archive runtime: startup can update Archive catalog/mark housekeeping.
It is **not a byte-for-byte read-only filesystem reader**. Archive's active-owner
checks remain enabled; never delete its mark file to bypass an ownership error.
The temporary Media Driver is isolated and removed when inspection finishes or
fails. No live order subscription or gateway is started.

## Run it

Use Java 25. From the repository root, build and obtain the runtime classpath:

```sh
mvn package dependency:build-classpath -Dmdep.outputFile=target/runtime-classpath.txt
```

Then inspect an existing directory that contains `archive.catalog`:

```sh
java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "target/classes:$(cat target/runtime-classpath.txt)" \
  dev.sam.exchange.audit.ArchiveInspector \
  --archive=/absolute/path/to/exchange-archive --levels=10
```

`--archive=path` is required. There is no default that could quietly create a new
history. `--levels` accepts 1 through 1000, defaults to 10, and limits each side
independently. `--format=text` is the default; `--format=json` produces one JSON
object and a newline. Unknown flags, repeated flags and invalid values fail before
opening resources. Standalone `--help` prints usage without an Archive.

Use the same Java invocation with `--format=json` and redirect its stdout to save
a report. Keep the report outside the Archive directory. Build output is separate
from the Java command; JVM diagnostics go to stderr. A complete report is rendered
only after successful replay and owned-runtime cleanup. A malformed recording,
unsupported message, ambiguous history, active owner or interrupted replay fails
with a nonzero exit and no report. An initialized Archive with no request recording
produces a valid empty report and does not create a request recording.

## What the numbers mean

| Field | Meaning |
| --- | --- |
| Observed requests | Every decoded request in the selected recording, in order. |
| Unique requests | First use of a UUID, including business rejections and missing-order cancellations. |
| Duplicate requests | Same UUID and same command again; returns the cached result. |
| Conflicting requests | Same UUID with a different command; leaves the original cached entry intact. |
| Place / cancel requests | Unique executions of each command type. |
| Business rejections | Unique rejected commands, such as a duplicate resting order ID. |
| Trades / executed lots | Fills from unique placement results. Cached responses do not add trades again. |
| Notional tick-lots | Exact sum of each trade's `priceTicks × quantityLots`; no currency conversion. |
| Book totals | All resting orders and price levels, with their remaining quantities. |
| Depth | Best bids first (descending price), best asks first (ascending price), limited per side. |

Observed = unique + duplicate + conflicting. Place + cancel = unique. Business
rejections are a subset of unique executions, not an additional request category.
Cancelling an absent order is a successful command result with `cancelled=false`,
not a business rejection. The normal live engine avoids appending cached retries
and conflicts; their counts are usually zero. Inspection still classifies them
correctly if a history contains them.

Book totals include levels omitted from depth. Quantities are remaining lots,
not the original submitted size. Aggregated lots and notional use `BigInteger`;
adding multiple maximum-sized orders does not wrap a signed 64-bit total.

## JSON contract: format version 1

The top-level fields are `formatVersion`, `historyDigest`, `requests`, `trades`,
`book` and `depth`. Depth contains `bids` and `asks` arrays of objects with
`priceTicks`, `orderCount` and `quantityLots`. Empty sides are empty arrays.

All Java `long` values and arbitrary-precision totals are **decimal strings**.
This includes request/trade counters and prices. Version numbers and book/level
order counts are JSON numbers because they are bounded Java `int` values. Parse
wide values with an exact integer/decimal type; do not convert them through a
JavaScript `Number`. Reports contain no request UUIDs or individual order IDs.
Field order and numeric formatting are deterministic and independent of locale.

## Comparing histories

`historyDigest.algorithm` is `sha256-chain-sbe-v1`. Its lowercase hex `value`
fingerprints the ordered decoded requests using the current SBE codec's canonical
encoding, including the UUID and command. It is not a hash of Archive segment
files, padding, transport headers or housekeeping metadata.

The algorithm is reproducible:

1. Start with `SHA-256(UTF-8("EXCHANGE-REQUEST-HISTORY-V1\n"))`.
2. Encode each decoded request with `SbeRequestCodec`, including its SBE message
   header. Let `frame` be those bytes.
3. Set the digest to `SHA-256(previous 32 raw digest bytes || uint32 little-endian
   frame length || frame)`.
4. Render the final 32 bytes as lowercase hexadecimal.

The empty-history digest is:

```text
b2f9064c93325a748e6dbbe28931d844f9c50e1612a109251001f02f45e0ec5c
```

Changing request order, UUID, payload or adding a duplicate observation changes
the digest. Compare both the algorithm label and value from reports made with
compatible codecs. Different histories can produce the same final book, so a
history difference is not proof of a book difference. This digest is neither a
signature nor an independently authenticated integrity guarantee.

## Operational limits

Replay streams the recording rather than retaining a request list. The reconstructed
book and UUID response cache still grow with history, just as they do during normal
recovery. Depth bounds output size, not replay memory or runtime. Run large
inspections with the same memory planning as recovery.

Queries run on the owning thread; returned projections are immutable. The Java
`inspect(AeronArchive, levels)` overload borrows an already-open client and leaves
its lifecycle to the caller. Serialize that client's use and ensure its request
recording is stopped. The path overload and CLI own their private runtime.

Interruption is preserved. Failed replay may have applied an in-memory prefix;
that partial state is discarded and no successful report is returned. Preserve
recordings and diagnose the original exception before retrying. Inspection does
not repair data, truncate recordings, add replication or replace operational
backups. See [Archive recovery and failure handling](archive-recovery.md).
