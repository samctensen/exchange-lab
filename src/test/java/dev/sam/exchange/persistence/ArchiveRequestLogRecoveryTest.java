package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.CommandRequest;
import io.aeron.CommonContext;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
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

  @Test
  void rejectsInterruptedOpenBeforeCreatingAnyRecording(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      long correlationId = f.archive.lastCorrelationId();
      Thread.currentThread().interrupt();
      try {
        assertThrows(IllegalStateException.class, () -> {
          try (ArchiveRequestLog unexpected = ArchiveRequestLog.open(f.archive, r -> fail("No recovery expected"))) {
            fail("Interrupted open created recording " + unexpected.recordingId());
          }
        });
        assertTrue(Thread.currentThread().isInterrupted());
        assertEquals(correlationId, f.archive.lastCorrelationId(), "Interrupted open must not query the catalogue");
      } finally {
        Thread.interrupted();
      }
      assertEquals(0, f.archive.listRecordings(0, 10, (a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q) -> {
      }));
      try (ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail("History must remain empty"))) {
        assertTrue(log.recordingId() >= 0);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,1", "false,3", "false,20", "true,1", "true,3", "true,20"})
  void interruptionInRecoveryStopsBeforeTheNextCommand(boolean openWriter, int requestCount, @TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      List<CommandRequest> expected = new ArrayList<>();
      for (int i = 1; i <= requestCount; i++)
        expected.add(request(i));
      Recorded saved = record(f, "exchange-request-log", expected.toArray(CommandRequest[]::new));
      List<CommandRequest> recovered = new ArrayList<>();
      java.util.function.Consumer<CommandRequest> recovery = request -> {
        recovered.add(request);
        Thread.currentThread().interrupt();
      };
      try {
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
          if (openWriter) {
            try (ArchiveRequestLog unexpected = ArchiveRequestLog.open(f.archive, recovery)) {
              fail("Interrupted recovery opened recording " + unexpected.recordingId());
            }
          } else {
            ArchiveRequestLog.replay(f.archive, recovery);
          }
        });
        assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
        assertTrue(Thread.currentThread().isInterrupted());
        assertEquals(List.of(request(1)), recovered, "Interruption must stop within the current poll batch");
      } finally {
        Thread.interrupted();
      }
      assertEquals(saved.stop(), f.archive.getStopPosition(saved.id()),
          "Interrupted recovery must leave history stopped");
      recovered.clear();
      ArchiveRequestLog.replay(f.archive, recovered::add);
      assertEquals(expected, recovered);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,1", "false,2", "true,1", "true,2"})
  void callbackFailurePropagatesEvenWhenTheBorrowedClientLogsSubscriberErrors(boolean openWriter, int failAt,
      @TempDir Path directory) {
    List<Throwable> subscriberErrors = new ArrayList<>();
    try (Fixture f = new Fixture(directory, subscriberErrors::add)) {
      List<CommandRequest> history = List.of(request(1), request(2));
      Recorded saved = record(f, "exchange-request-log", history.toArray(CommandRequest[]::new));
      IllegalArgumentException cause = new IllegalArgumentException("Cannot apply recovered request");
      List<CommandRequest> attempted = new ArrayList<>();
      java.util.function.Consumer<CommandRequest> recovery = request -> {
        attempted.add(request);
        if (attempted.size() == failAt)
          throw cause;
      };
      assertSame(cause, assertThrows(IllegalArgumentException.class, () -> {
        if (openWriter) {
          try (ArchiveRequestLog unexpected = ArchiveRequestLog.open(f.archive, recovery)) {
            fail("Failed recovery opened recording " + unexpected.recordingId());
          }
        } else {
          ArchiveRequestLog.replay(f.archive, recovery);
        }
      }));
      assertEquals(history.subList(0, failAt), attempted, "No command after the failure may be applied");
      assertEquals(saved.stop(), f.archive.getStopPosition(saved.id()));
      assertTrue(subscriberErrors.isEmpty(), "Recovery failures belong to the caller, not the Aeron error handler");
      List<CommandRequest> recovered = new ArrayList<>();
      try (ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, recovered::add)) {
        assertEquals(saved.id(), log.recordingId());
        assertEquals(history, recovered);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void malformedHistoryReportsItsRecordingAndFramePositionBeforeOpeningAWriter(boolean openWriter,
      @TempDir Path directory) {
    List<Throwable> subscriberErrors = new ArrayList<>();
    try (Fixture f = new Fixture(directory, subscriberErrors::add)) {
      String privatePayload = "private-order-payload";
      Recorded saved = recordFrames(f, "exchange-request-log", encoded(request(1)),
          privatePayload.getBytes(StandardCharsets.US_ASCII));
      for (int attempt = 0; attempt < 2; attempt++) {
        List<CommandRequest> recovered = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
          if (openWriter) {
            try (ArchiveRequestLog unexpected = ArchiveRequestLog.open(f.archive, recovered::add)) {
              fail("Malformed history opened recording " + unexpected.recordingId());
            }
          } else {
            ArchiveRequestLog.replay(f.archive, recovered::add);
          }
        });
        assertTrue(failure.getMessage().contains("recording " + saved.id()), failure.getMessage());
        assertTrue(failure.getMessage().contains("position " + saved.stop()), failure.getMessage());
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertFalse((failure.toString() + failure.getCause()).contains(privatePayload));
        assertEquals(List.of(request(1)), recovered);
        assertEquals(saved.stop(), f.archive.getStopPosition(saved.id()), "Malformed history must remain stopped");
      }
      assertTrue(subscriberErrors.isEmpty());
    }
  }

  @Test
  void interruptedCloseStopsTheOwnedRecordingAndPreservesRecoverableHistory(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail("New history must be empty"));
      long[] offered = {-1};
      await(() -> (offered[0] = log.offer(request(1))) >= 0);
      await(() -> log.isRecorded(offered[0]));
      long id = log.recordingId();
      Thread.currentThread().interrupt();
      try {
        IllegalStateException failure = assertThrows(IllegalStateException.class, log::close);
        assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }
      assertEquals(offered[0], f.archive.getStopPosition(id), "Failed close must still finish recording cleanup");
      assertThrows(IllegalStateException.class, () -> log.offer(request(2)));
      List<CommandRequest> recovered = new ArrayList<>();
      try (ArchiveRequestLog reopened = ArchiveRequestLog.open(f.archive, recovered::add)) {
        assertEquals(id, reopened.recordingId());
        assertEquals(List.of(request(1)), recovered);
      }
    }
  }

  @Test
  void closePreservesItsPrimaryFailureWhenStoppingTheRecordingAlsoFails(@TempDir Path directory) {
    try (Fixture f = new Fixture(directory)) {
      Aeron aeron = f.archive.context().aeron();
      // Keep the borrowed Aeron client alive so only the Archive control connection fails.
      f.archive.context().ownsAeronClient(false);
      try (aeron; Subscription observer = aeron.addSubscription("aeron:ipc?alias=exchange-request-log", 2001)) {
        ArchiveRequestLog log = ArchiveRequestLog.open(f.archive, r -> fail("New history must be empty"));
        await(observer::isConnected);
        f.archive.close();
        Thread.currentThread().interrupt();
        try {
          IllegalStateException failure = assertThrows(IllegalStateException.class, log::close);
          assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
          assertEquals(1, failure.getSuppressed().length, "The failed stop must be attached to the interruption");
          assertTrue(Thread.currentThread().isInterrupted());
        } finally {
          Thread.interrupted();
        }
        await(() -> !observer.isConnected());
        log.close();
      }
    }
  }

  private record Recorded(long id, long stop) {
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0, id), new CancelOrder(id));
  }

  private static byte[] encoded(CommandRequest request) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    int length = new SbeRequestCodec().encode(request, buffer, 0);
    byte[] bytes = new byte[length];
    buffer.getBytes(0, bytes);
    return bytes;
  }

  private static Recorded record(Fixture f, String alias, CommandRequest... requests) {
    byte[][] frames = new byte[requests.length][];
    for (int i = 0; i < requests.length; i++)
      frames[i] = encoded(requests[i]);
    return recordFrames(f, alias, frames);
  }

  private static Recorded recordFrames(Fixture f, String alias, byte[]... frames) {
    try (ExclusivePublication publication = f.archive.addRecordedExclusivePublication("aeron:ipc?alias=" + alias,
        2001)) {
      CountersReader counters = f.archive.context().aeron().countersReader();
      await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
          f.archive.archiveId()) != CountersReader.NULL_COUNTER_ID);
      int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), f.archive.archiveId());
      long recordingId = RecordingPos.getRecordingId(counters, counterId);
      long[] position = {publication.position()};
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
      for (byte[] frame : frames) {
        buffer.putBytes(0, frame);
        await(() -> (position[0] = publication.offer(buffer, 0, frame.length)) >= 0);
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
      this(directory, null);
    }
    Fixture(Path directory, ErrorHandler subscriberErrorHandler) {
      driver = ArchivingMediaDriver.launch(
          new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName())
              .ipcTermBufferLength(64 * 1024).dirDeleteOnShutdown(true),
          new Archive.Context().archiveDir(directory.toFile()).deleteArchiveOnStart(false).controlChannelEnabled(false)
              .localControlChannel("aeron:ipc").recordingEventsEnabled(false)
              .replicationChannel("aeron:udp?endpoint=localhost:0")
              .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc")).fileSyncLevel(2)
              .catalogFileSyncLevel(2));
      AeronArchive.Context context = new AeronArchive.Context()
          .aeronDirectoryName(driver.mediaDriver().aeronDirectoryName()).controlRequestChannel("aeron:ipc")
          .controlRequestStreamId(driver.archive().context().localControlStreamId())
          .controlResponseChannel("aeron:ipc");
      if (subscriberErrorHandler != null) {
        context.aeron(Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.mediaDriver().aeronDirectoryName())
            .subscriberErrorHandler(subscriberErrorHandler))).ownsAeronClient(true);
      }
      archive = AeronArchive.connect(context);
    }
    public void close() {
      archive.close();
      driver.close();
    }
  }
}
