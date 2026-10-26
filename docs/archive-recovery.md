# Archive recovery and failure handling

The engine restores one ordered request log before accepting live requests.
The recording must have exact channel alias `exchange-request-log` on stream
`2001`. Catalogue lookup pages through matching descriptions and compares the
parsed alias exactly. Similar aliases are ignored; two exact matches are an
error, not a reason to choose the newest recording.

A usable recording must be stopped and start at position zero. Startup replays
its requests into a fresh state machine, then extends that same recording.
See [the architecture](architecture.md) and [recording guarantees](../README.md#ordering-recording-and-recovery)
for the write-before-execution sequence and local durability limits.

For aggregate outcomes, bounded book depth and a deterministic history fingerprint,
use the [offline Archive inspector](archive-inspection.md). Stop the current owner
first; inspection does not append requests, but Archive startup can update its own
housekeeping metadata.

## Cancellation and corrupt history

- An already interrupted caller fails before catalogue access or writer creation.
- If a recovery callback interrupts its thread, replay stops before the next
  request, preserves the flag and fails even when that was the final request.
- A callback runtime failure is rethrown unchanged. It cannot be swallowed by a
  borrowed Aeron client's subscriber error handler that only logs exceptions.
- An SBE decode failure identifies the recording ID and the **frame end position**,
  retains the decode exception as its cause and does not include raw payloads.
  Positions are framed stream byte positions, not request indexes.

A failed recovery can have applied a prefix to the supplied in-memory state.
Discard that partial state and restart recovery with a fresh state machine.
The saved recording remains authoritative; a failure does not authorize deleting
segments, skipping a request or opening a new history. Inspect the exception and
preserve the Archive before diagnosing malformed data or schema incompatibility.

## Closing the log

A successful close waits for the last offered request to be recorded, stops the
owned recording and confirms its stop position covers that offer. If the flush
fails, close still attempts to stop the recording and close the owned publication.
The original failure is retained; additional cleanup failures are suppressed on it.
Failed cleanup never becomes a successful recording acknowledgement.

An existing interrupt is temporarily cleared for bounded cleanup and restored
before close returns or throws. This allows the Archive stop RPC to finish;
interrupted close can therefore still wait for cleanup. The borrowed Archive
client belongs to the caller and must remain open until log cleanup completes.

A failed or interrupted close leaves request outcomes uncertain. Keep each
original UUID and command and reconcile after recovery. This change preserves
local Archive guarantees; it adds no replication, snapshot retention or protection
against a storage device that fails to honor forced writes.
