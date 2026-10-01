package dev.sam.exchange.transport;

import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.status.CountersReader;

import io.aeron.Aeron;
import io.aeron.archive.ArchiveCounters;

/** Read-only benchmark diagnostics for a dedicated Archive on the connected Media Driver. */
final class ArchiveWriteCounters {
  private static final long SETTLE_NS = TimeUnit.MILLISECONDS.toNanos(20);
  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
  private final CountersReader counters;
  private final long archiveId;
  private final List<Identity> identities = new ArrayList<>();

  record Snapshot(long bytes, long totalWriteNanos, long maxWriteNanos) {
  }
  private record Identity(int id, int type, long registration) {
  }

  static ArchiveWriteCounters find(CountersReader counters) {
    List<Long> archives = new ArrayList<>();
    counters.forEach((id, type, key, label) -> {
      if (type == ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID)
        archives.add(key.getLong(0));
    });
    if (archives.size() != 1)
      throw new IllegalStateException("Archive counters require exactly one Archive on a dedicated benchmark driver");
    return new ArchiveWriteCounters(counters, archives.getFirst());
  }

  private ArchiveWriteCounters(CountersReader counters, long archiveId) {
    this.counters = counters;
    this.archiveId = archiveId;
    for (int type : new int[]{ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
        ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID}) {
      int id = ArchiveCounters.find(counters, type, archiveId);
      if (id == Aeron.NULL_VALUE)
        throw new IllegalStateException("Missing Archive counter type " + type + " for archive " + archiveId);
      identities.add(new Identity(id, type, counters.getCounterRegistrationId(id)));
    }
  }

  Snapshot awaitSnapshot(long expectedBytes) {
    return awaitSnapshot(expectedBytes, SystemNanoClock.INSTANCE,
        new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1)));
  }

  Snapshot awaitSnapshot(long expectedBytes, NanoClock clock, IdleStrategy idle) {
    long started = clock.nanoTime();
    long stableSince = started;
    Snapshot previous = null;
    while (true) {
      long now = clock.nanoTime();
      if (now - started >= TIMEOUT_NS)
        throw new IllegalStateException("Timed out waiting for Archive statistics at " + expectedBytes + " bytes");
      Snapshot sample = read();
      if (sample.bytes() > expectedBytes)
        throw new IllegalStateException("Unexpected Archive writes; use a fresh dedicated benchmark server");
      if (!sample.equals(previous)) {
        previous = sample;
        stableSince = now;
      }
      // RecordingPos can advance before the recorder publishes its statistics. These counters are not an atomic
      // tuple: require the expected byte total and a quiet observation period, outside the benchmark timers.
      if (sample.bytes() == expectedBytes && now - stableSince >= SETTLE_NS)
        return sample;
      idle.idle();
    }
  }

  private Snapshot read() {
    validateIdentities();
    Snapshot sample = new Snapshot(counters.getCounterValue(identities.get(0).id()),
        counters.getCounterValue(identities.get(1).id()), counters.getCounterValue(identities.get(2).id()));
    validateIdentities();
    if (sample.bytes() < 0 || sample.totalWriteNanos() < 0 || sample.maxWriteNanos() < 0)
      throw new IllegalStateException("Archive statistics are invalid");
    return sample;
  }

  private void validateIdentities() {
    for (Identity counter : identities) {
      if (counters.getCounterState(counter.id()) != CountersReader.RECORD_ALLOCATED
          || counters.getCounterRegistrationId(counter.id()) != counter.registration()
          || counters.getCounterTypeId(counter.id()) != counter.type() || counters.metaDataBuffer()
              .getLong(CountersReader.metaDataOffset(counter.id()) + CountersReader.KEY_OFFSET) != archiveId)
        throw new IllegalStateException("Archive counter closed or replaced during measurement");
    }
  }

  String summarize(Snapshot before, Snapshot after, long elapsedNanos) {
    if (elapsedNanos <= 0)
      throw new IllegalArgumentException("Measured duration must be positive");
    if (after.bytes() < before.bytes() || after.totalWriteNanos() < before.totalWriteNanos()
        || after.maxWriteNanos() < before.maxWriteNanos())
      throw new IllegalStateException("Archive counters decreased during measurement");
    long writeNanos = after.totalWriteNanos() - before.totalWriteNanos();
    return String.format(Locale.ROOT,
        "Archive ID: %d%nArchive write bytes (measured phase): %d%nArchive write time (measured phase): %d ns%n"
            + "Archive write time / client elapsed: %.3f%%%nArchive max write time before (lifetime): %d ns%n"
            + "Archive max write time after (lifetime): %d ns%n",
        archiveId, after.bytes() - before.bytes(), writeNanos, 100.0 * writeNanos / elapsedNanos,
        before.maxWriteNanos(), after.maxWriteNanos());
  }
}
