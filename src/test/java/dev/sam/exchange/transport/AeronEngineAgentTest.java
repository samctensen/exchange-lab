package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.persistence.RequestLog;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import org.agrona.concurrent.NanoClock;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

@Timeout(15)
class AeronEngineAgentTest {
  @Test
  void retainsPendingReplyWithoutReprocessingOrTakingTheNextRequest(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      assertEquals(0, server.agent.doWork(), "An idle pass must report no work");
      PlaceOrder bid = new PlaceOrder(1L, Side.BID, 100L, 10L);
      CommandRequest first = new CommandRequest(new UUID(0L, 1L), bid);
      CommandRequest second = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(2L, Side.ASK, 99L, 4L));
      server.send(first);
      server.send(second);

      // There is no reply subscriber yet. Processing must return with the first response pending.
      assertTimeout(Duration.ofSeconds(1), () -> server.awaitProcessed(1));
      assertTimeout(Duration.ofSeconds(1), () -> {
        for (int i = 0; i < 20; i++) {
          assertEquals(0, server.agent.doWork(), "An unsuccessful offer makes no progress");
        }
      });
      assertEquals(List.of(first), server.processor.processed);
      assertEquals(List.of(first), server.log.accepted);
      assertEquals(List.of(new OrderSnapshot(bid, 10L)), server.book.snapshot());

      // Connecting later must release the original response, then let the queued request run.
      try (Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
        awaitConnected(server.replies);
        assertEquals(
            List.of(new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L)),
                new CommandResponse(second.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L))),
            server.awaitResponses(responses, 2));
      }
      assertEquals(List.of(first, second), server.processor.processed);
      assertEquals(List.of(first, second), server.log.accepted);
      assertEquals(List.of(new OrderSnapshot(bid, 6L)), server.book.snapshot());
      assertEquals(0, server.agent.doWork());
      assertFalse(server.requests.isClosed(), "The server still owns the borrowed subscription");
      assertFalse(server.replies.isClosed(), "The server still owns the borrowed publication");
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void countsFragmentsAsWorkButProcessesOnlyTheCompleteRequest(@TempDir Path tempDir) throws Exception {
    // A 64-byte MTU leaves 32 bytes of payload; a valid SBE place request needs 49 bytes.
    try (TestServer server = new TestServer(tempDir, 64)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
      int length = server.send(request);
      assertTrue(length > server.commands.maxPayloadLength(), "The fixture must fragment without changing the message");
      int partialPasses = 0;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();

      while (server.processor.processed.isEmpty()) {
        int work = server.agent.doWork();
        if (work > 0 && server.processor.processed.isEmpty()) {
          partialPasses++;
          assertEquals(List.of(), server.log.accepted);
          assertEquals(List.of(), server.book.snapshot());
        }
        assertTrue(System.nanoTime() - deadline < 0, "The complete request was never processed");
        idle.idle(work);
      }

      assertTrue(partialPasses > 0, "The fixture must span several polling passes");
      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.log.accepted);
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  void rejectsIncorrectRequestFrameLengthBeforeRecordingOrProcessing(int lengthAdjustment, @TempDir Path tempDir)
      throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
      buffer.setMemory(0, 256, (byte) 0x55);
      int length = new SbeRequestCodec().encode(request, buffer, 0);
      // The backing buffer holds a complete request. Only the offered frame length is invalid.
      server.sendEncoded(buffer, length + lengthAdjustment);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      while (server.errors.isEmpty()) {
        int work = server.agent.doWork();
        assertTrue(System.nanoTime() - deadline < 0, "Invalid request was not rejected");
        idle.idle(work);
      }

      assertInstanceOf(IllegalArgumentException.class, server.errors.peek());
      assertEquals(0, server.log.offers);
      assertTrue(server.processor.processed.isEmpty());
      assertTrue(server.book.snapshot().isEmpty());
    }
  }

  @Test
  void repeatedOffersKeepTheOriginalFiveSecondDeadline(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      server.send(request);
      server.awaitProcessed(1);
      server.clock.advance(TimeUnit.SECONDS.toNanos(5));
      long started = System.nanoTime();
      long testDeadline = started + TimeUnit.SECONDS.toNanos(7);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();

      IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
        while (true) {
          assertEquals(0, server.agent.doWork());
          assertTrue(System.nanoTime() - testDeadline < 0, "Reply retries kept extending the deadline");
          idle.idle();
        }
      });

      assertTrue(failure.getMessage().contains("Timed out sending reply"), failure.getMessage());

      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.log.accepted);
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void aClosedReplyPublicationFailsInsteadOfRetrying(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      server.send(request);
      server.awaitProcessed(1);
      server.replies.close();

      IllegalStateException failure = assertThrows(IllegalStateException.class, server.agent::doWork);

      assertEquals("Publication is closed", failure.getMessage());
      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.log.accepted);
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void waitsForRecordingBeforeApplyingOrReplyingAndDoesNotOfferTwice(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir);
        Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
      awaitConnected(server.replies);
      server.log.recordImmediately = false;
      CommandRequest first = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
      CommandRequest second = new CommandRequest(new UUID(0, 2), new CancelOrder(1));
      server.send(first);
      server.send(second);
      server.awaitOffered();
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(List.of(first), server.log.accepted);
      assertEquals(1, server.log.offers);
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.book.snapshot());
      assertEquals(0, responses.poll((b, o, l, h) -> {
        throw new AssertionError("Unrecorded request received a reply");
      }, 10));

      server.log.recordedPosition = 64;
      assertEquals(List.of(new CommandResponse(first.requestId(), new PlaceResult(1, List.of(), 10))),
          server.awaitResponses(responses, 1));
      assertEquals(List.of(first), server.processor.processed);
      assertEquals(1L, server.log.accepted.stream().filter(first::equals).count());
    }
  }

  @Test
  void logBackPressureRetainsRequestAndSuccessfulOfferHasItsOwnRecordingDeadline(@TempDir Path tempDir)
      throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      server.log.backPressure = true;
      server.log.recordImmediately = false;
      CommandRequest request = new CommandRequest(new UUID(0, 1), new CancelOrder(1));
      server.send(request);
      server.awaitOffered();
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.log.accepted);
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      assertEquals(0, server.agent.doWork());
      server.log.backPressure = false;
      assertTrue(server.agent.doWork() > 0);
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      assertEquals(0, server.agent.doWork());
      server.log.recordedPosition = 64;
      server.awaitProcessed(1);
      assertEquals(List.of(request), server.log.accepted);
    }
  }

  @Test
  void recordingTimeoutOrFailureNeverAppliesRequest(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      server.log.recordImmediately = false;
      server.send(new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10)));
      server.awaitOffered();
      server.clock.advance(TimeUnit.SECONDS.toNanos(5));
      IllegalStateException failure = assertThrows(IllegalStateException.class, server.agent::doWork);
      assertTrue(failure.getMessage().contains("recording"), failure.getMessage());
      assertEquals(List.of(), server.book.snapshot());
      assertEquals(List.of(), server.processor.processed);
    }
  }

  @Test
  void stoppedRecordingNeverAppliesPendingRequest(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      server.log.recordImmediately = false;
      server.send(new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10)));
      server.awaitOffered();
      server.log.failure = new IllegalStateException("Recording failed");
      assertEquals(server.log.failure, assertThrows(IllegalStateException.class, server.agent::doWork));
      assertEquals(List.of(), server.book.snapshot());
      assertEquals(List.of(), server.processor.processed);
    }
  }

  @Test
  void logOfferTimeoutKeepsItsOriginalDeadline(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      server.log.backPressure = true;
      server.send(new CommandRequest(new UUID(0, 1), new CancelOrder(1)));
      server.awaitOffered();
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      for (int i = 0; i < 10; i++)
        assertEquals(0, server.agent.doWork());
      server.clock.advance(TimeUnit.SECONDS.toNanos(1));
      IllegalStateException failure = assertThrows(IllegalStateException.class, server.agent::doWork);
      assertTrue(failure.getMessage().contains("offering request"), failure.getMessage());
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.log.accepted);
    }
  }

  private static final class TestClock implements NanoClock {
    private long elapsed;
    public long nanoTime() {
      return elapsed;
    }
    void advance(long nanos) {
      elapsed += nanos;
    }
  }

  private static final class ControlledLog implements RequestLog {
    private final List<CommandRequest> accepted = new ArrayList<>();
    private int offers;
    private boolean recordImmediately = true;
    private boolean backPressure;
    private long recordedPosition;
    private RuntimeException failure;
    public long offer(CommandRequest request) {
      offers++;
      if (failure != null)
        throw failure;
      if (backPressure)
        return Publication.BACK_PRESSURED;
      accepted.add(request);
      long position = accepted.size() * 64L;
      if (recordImmediately)
        recordedPosition = position;
      return position;
    }
    public boolean isRecorded(long position) {
      if (failure != null)
        throw failure;
      return recordedPosition >= position;
    }
  }

  private static void awaitConnected(Publication publication) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!publication.isConnected()) {
      assertTrue(System.nanoTime() - deadline < 0, "Aeron publication did not connect");
      idle.idle();
    }
  }

  // Keep the real matching engine; record calls so deduplication cannot hide reprocessing.
  private static final class RecordingProcessor extends RequestStateMachine {
    private final List<CommandRequest> processed = new ArrayList<>();

    RecordingProcessor(OrderBook book) {
      super(new MatchingEngine(book));
    }

    @Override
    public CommandResponse process(CommandRequest request) {
      processed.add(request);
      return super.process(request);
    }
  }

  private static final class TestServer implements AutoCloseable {
    private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private final OrderBook book = new OrderBook();
    private final ControlledLog log = new ControlledLog();
    private final TestClock clock = new TestClock();
    private final RecordingProcessor processor;
    private final MediaDriver driver;
    private final Aeron aeron;
    private final Subscription requests;
    private final Publication replies;
    private final Publication commands;
    private final AeronEngineAgent agent;

    TestServer(Path tempDir) throws IOException {
      this(tempDir, 256);
    }

    TestServer(Path tempDir, int mtuLength) throws IOException {
      processor = new RecordingProcessor(book);
      driver = MediaDriver
          .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
              .ipcMtuLength(mtuLength).dirDeleteOnShutdown(true).errorHandler(errors::add));
      aeron = Aeron
          .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
      requests = aeron.addSubscription("aeron:ipc", 1);
      replies = aeron.addPublication("aeron:ipc", 2);
      commands = aeron.addPublication("aeron:ipc", 1);
      agent = new AeronEngineAgent(requests, replies, processor, log, false, clock);
      awaitConnected(commands);
    }

    void awaitOffered() throws IOException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (log.offers == 0) {
        agent.doWork();
        assertTrue(System.nanoTime() - deadline < 0, "Request was never offered to the log");
        Thread.onSpinWait();
      }
    }

    int send(CommandRequest request) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
      int length = new SbeRequestCodec().encode(request, buffer, 0);
      sendEncoded(buffer, length);
      return length;
    }

    void sendEncoded(DirectBuffer buffer, int length) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      while (commands.offer(buffer, 0, length) < 0) {
        assertTrue(System.nanoTime() - deadline < 0, "Test request could not be sent");
        idle.idle();
      }
    }

    void awaitProcessed(int count) throws IOException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      while (processor.processed.size() < count) {
        int work = agent.doWork();
        if (processor.processed.size() >= count) {
          assertTrue(work > 0, "Processing a command must count as work even when its reply cannot be sent");
        }
        assertTrue(System.nanoTime() - deadline < 0, "Test request was not processed");
        idle.idle(work);
      }
    }

    List<CommandResponse> awaitResponses(Subscription responses, int count) throws IOException {
      List<CommandResponse> received = new ArrayList<>();
      SbeResponseCodec codec = new SbeResponseCodec();
      FragmentAssembler assembler = new FragmentAssembler(
          (buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length)));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      while (received.size() < count) {
        int work = agent.doWork();
        work += responses.poll(assembler, 10);
        assertTrue(System.nanoTime() - deadline < 0, "Pending responses were not delivered");
        idle.idle(work);
      }
      return received;
    }

    @Override
    public void close() {
      commands.close();
      replies.close();
      requests.close();
      aeron.close();
      driver.close();
    }
  }
}
