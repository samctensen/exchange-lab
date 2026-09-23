package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import io.aeron.ExclusivePublication;
import io.aeron.archive.status.RecordingPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.*;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;

@Timeout(25)
class ArchiveRequestLogTest {
  @Test
  void recoversInOrderAndExtendsTheSameRecordingAcrossRestarts(@TempDir Path directory) {
    CommandRequest bid = request(1, new PlaceOrder(1, Side.BID, 100, 10));
    CommandRequest ask = request(2, new PlaceOrder(2, Side.ASK, 99, 4));
    long recordingId;
    long firstStop;
    try (Fixture f = new Fixture(directory)) {
      try (ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail("New log must be empty"))) {
        assertTrue(log.recordingId() >= 0);
        recordingId = log.recordingId();
        long first = append(log, bid);
        firstStop = append(log, ask);
        assertTrue(firstStop > first);
      }
      assertEquals(firstStop, f.archive.getStopPosition(recordingId));
    }
    OrderBook book = new OrderBook();
    RequestStateMachine state = new RequestStateMachine(new MatchingEngine(book));
    CommandRequest fill = request(3, new PlaceOrder(3, Side.ASK, 100, 6));
    try (Fixture f = new Fixture(directory);
        ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, state::process)) {
      assertEquals(recordingId, log.recordingId());
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) bid.command(), 6)), book.snapshot());
      assertEquals(new PlaceResult(2, List.of(new Trade(2, 1, 100, 4)), 0), state.process(ask).result());
      assertTrue(append(log, fill) > firstStop);
      assertEquals(new PlaceResult(3, List.of(new Trade(3, 1, 100, 6)), 0), state.process(fill).result());
    }
    List<CommandRequest> replayed = new ArrayList<>();
    try (Fixture f = new Fixture(directory)) {
      ArchiveRequestLog.replay(f.archive, replayed::add);
      assertEquals(List.of(bid, ask, fill), replayed);
      assertEquals(1, f.archive.listRecordings(0, 10, (a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q) -> {
      }));
    }
  }

  @Test
  void emptyRecordingSurvivesRepeatedRestart(@TempDir Path directory) {
    long id;
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      id = log.recordingId();
      assertTrue(id >= 0);
    }
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      assertEquals(id, log.recordingId());
      append(log, request(1, new CancelOrder(10)));
    }
  }

  @Test
  void recordedButUnappliedRequestsRecoverWithRetryAndRejectionSemantics(@TempDir Path directory) {
    CommandRequest ask = request(1, new PlaceOrder(1, Side.ASK, 100, 5));
    CommandRequest rejected = request(2, new PlaceOrder(1, Side.ASK, 90, 10));
    CommandRequest cancel = request(3, new CancelOrder(1));
    CommandRequest conflict = request(1, new PlaceOrder(99, Side.BID, 100, 5));
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      assertTrue(log.recordingId() >= 0);
      // Simulate stopping after recording, before application or reply. Replay tolerates repeated requests.
      for (CommandRequest request : List.of(ask, rejected, cancel, ask, conflict))
        append(log, request);
    }
    OrderBook book = new OrderBook();
    RequestStateMachine state = new RequestStateMachine(new MatchingEngine(book));
    try (Fixture f = new Fixture(directory);
        ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, state::process)) {
      assertTrue(log.recordingId() >= 0);
      assertTrue(book.snapshot().isEmpty());
      assertEquals(new PlaceResult(1, List.of(), 5), state.process(ask).result());
      assertEquals(new RejectResult(1, RejectReason.DUPLICATE_ORDER_ID), state.process(rejected).result());
      assertEquals(new RejectResult(99, RejectReason.REQUEST_ID_CONFLICT), state.process(conflict).result());
      assertTrue(book.snapshot().isEmpty());
    }
  }

  @Test
  void refusesToOfferOrAcknowledgeAfterRecordingStops(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      assertTrue(log.recordingId() >= 0);
      long position = append(log, request(1, new CancelOrder(1)));
      f.archive.tryStopRecordingByIdentity(log.recordingId());
      await(() -> f.archive.getStopPosition(log.recordingId()) != AeronArchive.NULL_POSITION);
      // Catalog stop and counter reclamation are separate asynchronous updates.
      await(() -> RecordingPos.findCounterIdByRecording(f.archive.context().aeron().countersReader(), log.recordingId(),
          f.archive.archiveId()) == CountersReader.NULL_COUNTER_ID);
      assertThrows(IllegalStateException.class, () -> log.offer(request(2, new CancelOrder(2))));
      assertThrows(IllegalStateException.class, () -> log.isRecorded(position + 64));
    }
  }

  @Test
  void recoveryFailurePreventsOpeningTheWriter(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      append(log, request(1, new CancelOrder(1)));
    }
    try (Fixture f = new Fixture(directory)) {
      IllegalArgumentException cause = new IllegalArgumentException("Cannot apply recovered request");
      assertSame(cause, assertThrows(IllegalArgumentException.class, () -> ArchiveRequestLog.open(f.archive, r -> {
        throw cause;
      })));
      List<CommandRequest> saved = new ArrayList<>();
      ArchiveRequestLog.replay(f.archive, saved::add);
      assertEquals(List.of(request(1, new CancelOrder(1))), saved);
    }
  }

  @Test
  void refusesASecondWriterWhileTheRecordingIsActive(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      long position = append(log, request(1, new CancelOrder(1)));
      IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> ArchiveRequestLog.open(f.archive, r -> fail("Active history must not be replayed")));
      assertTrue(failure.getMessage().contains("stopped"), failure.getMessage());
      assertTrue(append(log, request(2, new CancelOrder(2))) > position,
          "Refusing a second writer must leave the original recording usable");
    }
  }

  @Test
  void replaysAndExtendsAcrossATermBoundary(@TempDir Path directory) {
    List<CommandRequest> expected = new ArrayList<>();
    long recordingId;
    long position = 0;
    try (Fixture f = new Fixture(directory); ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
      recordingId = log.recordingId();
      for (int i = 0; i < 1100; i++) {
        CommandRequest request = request(i, new CancelOrder(i));
        expected.add(request);
        position = append(log, request);
      }
      assertTrue(position > 64 * 1024, "The fixture must rotate its 64 KiB term buffer");
    }
    List<CommandRequest> recovered = new ArrayList<>();
    try (Fixture f = new Fixture(directory);
        ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, recovered::add)) {
      assertEquals(recordingId, log.recordingId());
      assertEquals(expected, recovered);
      CommandRequest next = request(1100, new CancelOrder(1100));
      assertTrue(append(log, next) > position);
      expected.add(next);
    }
    recovered.clear();
    try (Fixture f = new Fixture(directory)) {
      ArchiveRequestLog.replay(f.archive, recovered::add);
      assertEquals(expected, recovered);
    }
  }

  @Test
  void refusesAmbiguousHistoryInsteadOfPickingARecording(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      try (ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail())) {
        append(log, request(1, new CancelOrder(1)));
      }
      // Create a second independent session with the same stream and log alias.
      long secondId;
      try (ExclusivePublication publication = f.archive
          .addRecordedExclusivePublication("aeron:ipc?alias=exchange-request-log", 2001)) {
        CountersReader counters = f.archive.context().aeron().countersReader();
        await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
            f.archive.archiveId()) != CountersReader.NULL_COUNTER_ID);
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), f.archive.archiveId());
        secondId = RecordingPos.getRecordingId(counters, counterId);
        f.archive.tryStopRecordingByIdentity(secondId);
        await(() -> f.archive.getStopPosition(secondId) != AeronArchive.NULL_POSITION);
      }
      IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> ArchiveRequestLog.open(f.archive, r -> fail("Ambiguous history must not be applied")));
      assertTrue(failure.getMessage().contains("Multiple request recordings"), failure.getMessage());
    }
  }

  private static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0, id), command);
  }

  private static long append(ArchiveRequestLog log, CommandRequest request) {
    long[] position = {-1};
    await(() -> (position[0] = log.offer(request)) >= 0);
    await(() -> log.isRecorded(position[0]));
    return position[0];
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
