package dev.sam.exchange.transport;

import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

class ArchiveWriteCountersTest {
  @Test
  void subtractsWarmupTotalsAndLabelsMaximaAsLifetimeValues() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    f.values(64, 12_000, 6_000);
    var before = reader.awaitSnapshot(64, f.clock, f.clock);
    f.values(192, 20_000, 6_000);
    var after = reader.awaitSnapshot(192, f.clock, f.clock);
    String report = reader.summarize(before, after, 100_000);
    assertTrue(report.contains("Archive ID: 7"), report);
    assertTrue(report.contains("Archive write bytes (measured phase): 128"), report);
    assertTrue(report.contains("Archive write time (measured phase): 8000 ns"), report);
    assertTrue(report.contains("Archive write time / client elapsed: 8.000%"), report);
    assertTrue(report.contains("Archive max write time before (lifetime): 6000 ns"), report);
    assertTrue(report.contains("Archive max write time after (lifetime): 6000 ns"), report);
  }

  @Test
  void waitsForTimeCountersToSettleAfterTheBytesCounterAdvances() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    f.values(64, 0, 0);
    f.clock.onIdle = () -> {
      if (f.clock.now >= TimeUnit.MILLISECONDS.toNanos(5))
        f.values(64, 3_000, 2_000);
    };
    var sample = reader.awaitSnapshot(64, f.clock, f.clock);
    assertEquals(new ArchiveWriteCounters.Snapshot(64, 3_000, 2_000), sample);
    assertTrue(f.clock.now >= TimeUnit.MILLISECONDS.toNanos(25));
  }

  @Test
  void refusesToMixCountersFromTwoArchives() {
    Fixture f = new Fixture();
    f.allocate(ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 8);
    assertThrows(IllegalStateException.class, () -> ArchiveWriteCounters.find(f.counters));
  }

  @Test
  void refusesAnIncompleteCounterSetEvenWhenAnotherArchiveHasTheMissingType() {
    Fixture f = new Fixture();
    f.counters.free(f.time);
    f.allocate(ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 8);
    assertThrows(IllegalStateException.class, () -> ArchiveWriteCounters.find(f.counters));
  }

  @Test
  void failsClearlyWhenThereIsNoArchive() {
    Fixture f = new Fixture();
    f.counters.free(f.bytes);
    assertThrows(IllegalStateException.class, () -> ArchiveWriteCounters.find(f.counters));
  }

  @Test
  void detectsClosedCountersInsteadOfReportingStaleValues() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    f.counters.free(f.time);
    assertThrows(IllegalStateException.class, () -> reader.awaitSnapshot(0, f.clock, f.clock));
  }

  @Test
  void detectsReusedCounterIdsEvenWithTheSameArchiveAndType() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    f.counters.setCounterRegistrationId(f.time, 999);
    assertThrows(IllegalStateException.class, () -> reader.awaitSnapshot(0, f.clock, f.clock));
  }

  @Test
  void timesOutWhenPublishedStatisticsNeverReachTheExpectedBytes() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> reader.awaitSnapshot(64, f.clock, f.clock));
    assertTrue(error.getMessage().contains("Timed out"), error.getMessage());
  }

  @Test
  void rejectsUnexpectedWritesFromOtherTraffic() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    f.values(128, 10, 10);
    assertThrows(IllegalStateException.class, () -> reader.awaitSnapshot(64, f.clock, f.clock));
  }

  @Test
  void rejectsDecreasingTotalsRatherThanPrintingNegativeDeltas() {
    Fixture f = new Fixture();
    ArchiveWriteCounters reader = ArchiveWriteCounters.find(f.counters);
    assertThrows(IllegalStateException.class, () -> reader.summarize(new ArchiveWriteCounters.Snapshot(64, 100, 100),
        new ArchiveWriteCounters.Snapshot(128, 50, 100), 1_000));
  }

  private static final class Fixture {
    final CountersManager counters = new CountersManager(
        new UnsafeBuffer(ByteBuffer.allocateDirect(16 * CountersReader.METADATA_LENGTH)),
        new UnsafeBuffer(ByteBuffer.allocateDirect(16 * CountersReader.COUNTER_LENGTH)));
    final int bytes = allocate(ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, 7);
    final int time = allocate(ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, 7);
    final int max = allocate(ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, 7);
    final TestClock clock = new TestClock();

    int allocate(int type, long archiveId) {
      int id = counters.allocate("Labels are not used for discovery", type, key -> key.putLong(0, archiveId));
      counters.setCounterRegistrationId(id, 100 + id);
      return id;
    }

    void values(long written, long nanos, long largest) {
      counters.setCounterValue(bytes, written);
      counters.setCounterValue(time, nanos);
      counters.setCounterValue(max, largest);
    }
  }

  private static final class TestClock implements NanoClock, IdleStrategy {
    long now;
    Runnable onIdle = () -> {
    };
    public long nanoTime() {
      return now;
    }
    public void idle() {
      now += TimeUnit.MILLISECONDS.toNanos(1);
      onIdle.run();
    }
    public void idle(int workCount) {
      idle();
    }
    public void reset() {
    }
  }
}
