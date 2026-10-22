package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.CommandRequest;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;

@Timeout(30)
class ArchiveRequestLogRecoveryTest {
  @Test
  void selectsTheExactAliasAfterSeveralPagesOfLookalikes(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      for (int i = 0; i < 17; i++)
        record(f, "exchange-request-log-backup-" + i, request(100 + i));
      Recorded expected = record(f, "exchange-request-log", request(1));
      List<CommandRequest> recovered = new ArrayList<>();
      try (ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, recovered::add)) {
        assertEquals(expected.id(), log.recordingId());
        assertEquals(List.of(request(1)), recovered);
      }
    }
  }

  @Test
  void detectsTwoExactAliasesBeyondLookalikeCatalogueEntries(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      record(f, "exchange-request-log", request(1));
      for (int i = 0; i < 17; i++)
        record(f, "exchange-request-log-backup-" + i, request(100 + i));
      record(f, "exchange-request-log", request(2));
      IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> ArchiveRequestLog.replay(f.archive, r -> fail("Ambiguous history must not be applied")));
      assertTrue(failure.getMessage().contains("Multiple request recordings"), failure.getMessage());
    }
  }

  @Test
  void rejectsInterruptedReplayBeforeCatalogueIoEvenForEmptyHistory(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      long correlationId = f.archive.lastCorrelationId();
      Thread.currentThread().interrupt();
      try {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> ArchiveRequestLog.replay(f.archive, r -> fail("Interrupted replay must not apply history")));
        assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
        assertTrue(Thread.currentThread().isInterrupted());
        assertEquals(correlationId, f.archive.lastCorrelationId(), "Interrupted replay must not query the catalogue");
      } finally {
        Thread.interrupted();
      }
      ArchiveRequestLog.replay(f.archive, r -> fail("History must remain empty"));
    }
  }

  private record Recorded(long id, long stop) {
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0, id), new CancelOrder(id));
  }

  private static Recorded record(Fixture f, String alias, CommandRequest... requests) {
    try (ExclusivePublication publication = f.archive.addRecordedExclusivePublication("aeron:ipc?alias=" + alias,
        2001)) {
      CountersReader counters = f.archive.context().aeron().countersReader();
      await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
          f.archive.archiveId()) != CountersReader.NULL_COUNTER_ID);
      int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), f.archive.archiveId());
      long recordingId = RecordingPos.getRecordingId(counters, counterId);
      long[] position = {publication.position()};
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
      SbeRequestCodec codec = new SbeRequestCodec();
      for (CommandRequest request : requests) {
        int length = codec.encode(request, buffer, 0);
        await(() -> (position[0] = publication.offer(buffer, 0, length)) >= 0);
      }
      await(() -> counters.getCounterValue(counterId) >= position[0]);
      f.archive.tryStopRecordingByIdentity(recordingId);
      await(() -> f.archive.getStopPosition(recordingId) == position[0]);
      return new Recorded(recordingId, position[0]);
    }
  }

  private static void await(BooleanSupplier done) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!done.getAsBoolean()) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for Archive");
      idle.idle();
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final ArchivingMediaDriver driver;
    private final AeronArchive archive;
    Fixture(Path directory) {
      driver = ArchivingMediaDriver.launch(
          new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName())
              .ipcTermBufferLength(64 * 1024).dirDeleteOnShutdown(true),
          new Archive.Context().archiveDir(directory.toFile()).deleteArchiveOnStart(false).controlChannelEnabled(false)
              .localControlChannel("aeron:ipc").recordingEventsEnabled(false)
              .replicationChannel("aeron:udp?endpoint=localhost:0")
              .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc")).fileSyncLevel(2)
              .catalogFileSyncLevel(2));
      archive = AeronArchive.connect(new AeronArchive.Context()
          .aeronDirectoryName(driver.mediaDriver().aeronDirectoryName()).controlRequestChannel("aeron:ipc")
          .controlRequestStreamId(driver.archive().context().localControlStreamId())
          .controlResponseChannel("aeron:ipc"));
    }
    public void close() {
      archive.close();
      driver.close();
    }
  }
}
