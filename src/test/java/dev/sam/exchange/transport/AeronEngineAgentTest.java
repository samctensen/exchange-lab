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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
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
  void stageTimingIncludesRetriesAndWaitsButExcludesCachedReplies(@TempDir Path tempDir) throws Exception {
    EngineStageTimings timings = new EngineStageTimings(0, 2);
    try (TestServer server = new TestServer(tempDir, 256, timings)) {
      CommandRequest request = new CommandRequest(new UUID(0, 1), new CancelOrder(1));
      server.clock.advance(10_000);
      server.log.backPressure = true;
      server.log.recordImmediately = false;
      server.send(request);
      server.awaitOffered();
      server.clock.advance(20_000);
      server.log.backPressure = false;
      server.awaitAccepted(1);
      server.clock.advance(30_000);
      server.log.recordedPosition = 64;
      server.processor.onProcess = () -> server.clock.advance(40_000);
      server.awaitProcessed(1);
      assertTrue(timings.summarize().contains("Stage timing: 0/2 samples"),
          "An applied command with an unsent reply is not a completed timing sample");

      server.clock.advance(50_000);
      try (Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
        awaitConnected(server.replies);
        server.awaitResponses(responses, 1);
        String report = timings.summarize();
        assertTrue(report.contains("Log offer: mean=20.000"), report);
        assertTrue(report.contains("Recording observation: mean=30.000"), report);
        assertTrue(report.contains("Process and size: mean=40.000"), report);
        assertTrue(report.contains("Reply offer: mean=50.000"), report);
        assertTrue(report.contains("Server total: mean=140.000"), report);

        server.send(request);
        server.awaitResponses(responses, 1);
        assertEquals(report, timings.summarize(), "Cached responses must not count as fresh logged samples");
      }
    }
  }

  @Test
  void stageTimingPreservesEachQueuedRequestsOwnTimestamps(@TempDir Path tempDir) throws Exception {
    EngineStageTimings timings = new EngineStageTimings(0, 2);
    try (TestServer server = new TestServer(tempDir, 256, timings)) {
      server.log.recordImmediately = false;
      server.send(new CommandRequest(new UUID(0, 1), new CancelOrder(1)));
      server.awaitAccepted(1);
      server.clock.advance(10_000);
      server.send(new CommandRequest(new UUID(0, 2), new CancelOrder(2)));
      server.awaitAccepted(2);
      server.clock.advance(20_000);
      server.log.recordedPosition = 128;
      server.awaitProcessed(1);
      server.clock.advance(40_000);
      try (Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
        awaitConnected(server.replies);
        server.awaitResponses(responses, 2);
      }
      String report = timings.summarize();
      assertTrue(report.contains("Stage timing: 2/2 samples"), report);
      assertTrue(report.contains("Log offer: mean=0.000"), report);
      assertTrue(report.contains("Recording observation: mean=45.000 p50=30.000 p99=60.000"), report);
      assertTrue(report.contains("Reply offer: mean=20.000 p50=0.000 p99=40.000"), report);
      assertTrue(report.contains("Server total: mean=65.000 p50=60.000 p99=70.000"), report);
    }
  }

  @Test
  void retainsRepliesWhileApplyingRecordedCommandsAndAdmittingMore(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      assertEquals(0, server.agent.doWork(), "An idle pass must report no work");
      PlaceOrder bid = new PlaceOrder(1L, Side.BID, 100L, 10L);
      CommandRequest first = new CommandRequest(new UUID(0L, 1L), bid);
      CommandRequest second = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(2L, Side.ASK, 99L, 4L));
      CommandRequest third = new CommandRequest(new UUID(0L, 3L), new CancelOrder(1L));
      server.log.recordImmediately = false;
      server.send(first);
      server.send(second);
      server.awaitAccepted(2);
      server.log.recordedPosition = 128;

      // There is no reply subscriber yet. Recorded commands still run and their replies queue in order.
      assertTimeout(Duration.ofSeconds(1), () -> server.awaitProcessed(1));
      server.send(third);
      assertTimeout(Duration.ofSeconds(1), () -> {
        server.awaitAccepted(3);
        server.awaitProcessed(2);
      });
      assertEquals(List.of(first, second), server.processor.processed);
      assertEquals(List.of(first, second, third), server.log.accepted);
      assertEquals(List.of(new OrderSnapshot(bid, 6L)), server.book.snapshot());
      assertEquals(0, server.agent.doWork(), "The third command still waits for recording");
      server.log.recordedPosition = 192;
      server.awaitProcessed(3);
      assertEquals(List.of(), server.book.snapshot());

      // Connecting later releases all original results, even though the book has changed since then.
      try (Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
        awaitConnected(server.replies);
        assertEquals(
            List.of(new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L)),
                new CommandResponse(second.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
                new CommandResponse(third.requestId(), new CancelResult(1L, true))),
            server.awaitResponses(responses, 3));
      }
      assertEquals(List.of(first, second, third), server.processor.processed);
      assertEquals(List.of(first, second, third), server.log.accepted);
      assertEquals(List.of(), server.book.snapshot());
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
  void repeatedOffersExpireOnlyTheirRouteAtTheOriginalFiveSecondDeadline(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      server.send(request);
      server.awaitProcessed(1);
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      for (int i = 0; i < 20; i++) {
        assertEquals(0, server.agent.doWork());
      }
      server.clock.advance(TimeUnit.SECONDS.toNanos(1));
      assertEquals(1, server.agent.doWork(), "Expiry removes the route without stopping the agent");
      assertTrue(server.replies.isClosed());
      assertEquals(0, server.agent.doWork());

      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.log.accepted);
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void aRemovedReplyPublicationKeepsTheOriginalDeadline(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      server.send(request);
      server.awaitProcessed(1);
      server.replies.close();

      // Aeron no longer returns a removed publication. An absent route must still time out.
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      assertEquals(0, server.agent.doWork());
      server.clock.advance(TimeUnit.SECONDS.toNanos(1));

      assertEquals(1, server.agent.doWork(), "Expiry discards the queued reply without stopping the agent");
      assertEquals(0, server.agent.doWork());
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
      server.awaitAccepted(2);
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(List.of(first, second), server.log.accepted);
      assertEquals(2, server.log.offers);
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.book.snapshot());
      assertEquals(0, responses.poll((b, o, l, h) -> {
        throw new AssertionError("Unrecorded request received a reply");
      }, 10));

      server.log.recordedPosition = 64;
      assertEquals(List.of(new CommandResponse(first.requestId(), new PlaceResult(1, List.of(), 10))),
          server.awaitResponses(responses, 1));
      assertEquals(List.of(first), server.processor.processed);
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) first.command(), 10)), server.book.snapshot());
      assertEquals(0, server.agent.doWork(), "The second request must wait for its own end position");

      server.log.recordedPosition = 128;
      assertEquals(List.of(new CommandResponse(second.requestId(), new CancelResult(1, true))),
          server.awaitResponses(responses, 1));
      assertEquals(List.of(first, second), server.processor.processed);
      assertEquals(2, server.log.offers, "Accepted requests must never be offered again");
      assertEquals(List.of(), server.book.snapshot());
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
      assertTrue(failure.getMessage().contains("64"), failure.getMessage());
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
      assertTrue(failure.getMessage().contains("offering"), failure.getMessage());
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.log.accepted);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void rejectsNonPositiveLogWindow(int logWindow) {
    assertThrows(IllegalArgumentException.class, () -> new AeronEngineAgent(null, null, null, null, false, logWindow));
  }

  @ParameterizedTest
  @CsvSource({"default, 8", "1, 1", "8, 8", "16, 16", "32, 32"})
  void boundsUnrecordedWindowAndRefillsOnlyFreedSlots(String configuredWindow, int window, @TempDir Path tempDir)
      throws Exception {
    Integer logWindow = configuredWindow.equals("default") ? null : Integer.valueOf(configuredWindow);
    int batchSize = window + 2;
    try (TestServer server = new TestServer(tempDir, 256, null, logWindow);
        Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
      awaitConnected(server.replies);
      server.log.recordImmediately = false;
      List<CommandRequest> batch = new ArrayList<>();
      List<CommandResponse> expected = new ArrayList<>();
      for (long id = 1; id <= batchSize; id++) {
        CommandRequest request = new CommandRequest(new UUID(0, id), new CancelOrder(id));
        batch.add(request);
        expected.add(new CommandResponse(request.requestId(), new CancelResult(id, false)));
        server.send(request);
      }

      server.awaitAccepted(window);
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(batch.subList(0, window), server.log.accepted);
      assertEquals(List.of(), server.processor.processed);

      server.log.recordedPosition = 64;
      assertEquals(expected.subList(0, 1), server.awaitResponses(responses, 1));
      server.awaitAccepted(window + 1);
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(batch.subList(0, window + 1), server.log.accepted, "Only one slot was freed");
      assertEquals(batch.subList(0, 1), server.processor.processed);

      server.log.recordImmediately = true;
      server.log.recordedPosition = (window + 1) * 64L;
      assertEquals(expected.subList(1, batchSize), server.awaitResponses(responses, batchSize - 1));
      assertEquals(batch, server.processor.processed);
      assertEquals(batch, server.log.accepted);
      assertEquals(batchSize, server.log.offers);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retryOfUnrecordedRequestWaitsWithoutAnotherLogEntry(boolean conflict, @TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir);
        Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
      awaitConnected(server.replies);
      server.log.recordImmediately = false;
      PlaceOrder order = new PlaceOrder(1, Side.BID, 100, 10);
      CommandRequest original = new CommandRequest(new UUID(0, 1), order);
      CommandRequest retry = conflict
          ? new CommandRequest(original.requestId(), new PlaceOrder(2, Side.BID, 100, 10))
          : original;
      CommandRequest cancel = new CommandRequest(new UUID(0, 3), new CancelOrder(1));
      server.send(original);
      server.send(retry);
      server.send(cancel);
      server.awaitAccepted(1);
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(List.of(original), server.log.accepted);
      assertEquals(1, server.log.offers);
      assertEquals(List.of(), server.processor.processed);
      assertEquals(List.of(), server.book.snapshot());

      CommandResponse originalResponse = new CommandResponse(original.requestId(), new PlaceResult(1, List.of(), 10));
      CommandResponse retryResponse = conflict
          ? new CommandResponse(original.requestId(), new RejectResult(2, RejectReason.REQUEST_ID_CONFLICT))
          : originalResponse;
      server.log.recordedPosition = 64;
      assertEquals(List.of(originalResponse, retryResponse), server.awaitResponses(responses, 2));
      server.awaitAccepted(2);
      assertEquals(List.of(original, cancel), server.log.accepted);
      assertEquals(List.of(new OrderSnapshot(order, 10)), server.book.snapshot());

      server.log.recordedPosition = 128;
      assertEquals(List.of(new CommandResponse(cancel.requestId(), new CancelResult(1, true))),
          server.awaitResponses(responses, 1));
      assertEquals(2, server.log.offers);
      assertEquals(List.of(), server.book.snapshot());
    }
  }

  @Test
  void cachedRetryCannotOvertakeAnOlderLoggedCancel(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir);
        Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
      awaitConnected(server.replies);
      CommandRequest original = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
      CommandResponse cached = new CommandResponse(original.requestId(), new PlaceResult(1, List.of(), 10));
      server.send(original);
      assertEquals(List.of(cached), server.awaitResponses(responses, 1));

      server.log.recordImmediately = false;
      CommandRequest cancel = new CommandRequest(new UUID(0, 2), new CancelOrder(1));
      server.send(cancel);
      server.send(original);
      server.awaitAccepted(2);
      for (int i = 0; i < 20; i++)
        assertEquals(0, server.agent.doWork());
      assertEquals(List.of(original), server.processor.processed);
      assertEquals(0, responses.poll((b, o, l, h) -> {
        throw new AssertionError("Cached retry overtook the unrecorded cancel");
      }, 10));

      server.log.recordedPosition = 128;
      assertEquals(List.of(new CommandResponse(cancel.requestId(), new CancelResult(1, true)), cached),
          server.awaitResponses(responses, 2));
      assertEquals(List.of(original, cancel), server.log.accepted);
      assertEquals(2, server.log.offers);
      assertEquals(List.of(), server.book.snapshot(),
          "The cached place response must not recreate the cancelled order");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void eachQueuedRequestKeepsItsOwnRecordingDeadline(boolean recordFirst, @TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir);
        Subscription responses = server.aeron.addSubscription("aeron:ipc", 2)) {
      awaitConnected(server.replies);
      server.log.recordImmediately = false;
      CommandRequest first = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
      CommandRequest second = new CommandRequest(new UUID(0, 2), new CancelOrder(1));
      server.send(first);
      server.awaitAccepted(1);
      server.clock.advance(TimeUnit.SECONDS.toNanos(4));
      server.send(second);
      server.awaitAccepted(2);

      if (recordFirst) {
        server.log.recordedPosition = 64;
        assertEquals(List.of(new CommandResponse(first.requestId(), new PlaceResult(1, List.of(), 10))),
            server.awaitResponses(responses, 1));
        server.clock.advance(TimeUnit.SECONDS.toNanos(4));
        assertEquals(0, server.agent.doWork(), "The second request has not reached its own deadline yet");
      }
      server.clock.advance(TimeUnit.SECONDS.toNanos(1));
      IllegalStateException failure = assertThrows(IllegalStateException.class, server.agent::doWork);
      assertTrue(failure.getMessage().contains(recordFirst ? "128" : "64"), failure.getMessage());
      assertEquals(recordFirst ? List.of(first) : List.of(), server.processor.processed);
      assertEquals(recordFirst ? List.of(new OrderSnapshot((PlaceOrder) first.command(), 10)) : List.of(),
          server.book.snapshot());
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
    private Runnable onProcess = () -> {
    };

    RecordingProcessor(OrderBook book) {
      super(new MatchingEngine(book));
    }

    @Override
    public CommandResponse process(CommandRequest request) {
      processed.add(request);
      onProcess.run();
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
    private final ResponsePublicationRegistry responsePublications;

    TestServer(Path tempDir) throws IOException {
      this(tempDir, 256);
    }

    TestServer(Path tempDir, int mtuLength) throws IOException {
      this(tempDir, mtuLength, null);
    }

    TestServer(Path tempDir, int mtuLength, EngineStageTimings timings) throws IOException {
      this(tempDir, mtuLength, timings, null);
    }

    TestServer(Path tempDir, int mtuLength, EngineStageTimings timings, Integer logWindow) throws IOException {
      processor = new RecordingProcessor(book);
      driver = MediaDriver
          .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
              .ipcMtuLength(mtuLength).dirDeleteOnShutdown(true).errorHandler(errors::add));
      aeron = Aeron
          .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
      requests = aeron.addSubscription("aeron:ipc", 1);
      responsePublications = new ResponsePublicationRegistry(aeron);
      // These ordering/timeout tests deliberately connect a plain reply subscriber later.
      // AeronEngineRoutingTest exercises the associated response subscriptions used by real clients.
      commands = aeron.addPublication("aeron:ipc", 1);
      agent = logWindow == null
          ? new AeronEngineAgent(requests, responsePublications, processor, log, false, clock, timings)
          : new AeronEngineAgent(requests, responsePublications, processor, log, false, clock, timings, logWindow);
      awaitConnected(commands);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (requests.imageCount() == 0) {
        assertTrue(System.nanoTime() - deadline < 0, "Request image did not arrive");
        Thread.yield();
      }
      agent.doWork();
      long route = requests.imageAtIndex(0).correlationId();
      while (responsePublications.find(route).isEmpty()) {
        assertTrue(System.nanoTime() - deadline < 0, "Response publication was not registered");
        Thread.yield();
      }
      replies = responsePublications.find(route).orElseThrow();
    }

    void awaitOffered() throws IOException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      while (log.offers == 0) {
        agent.doWork();
        assertTrue(System.nanoTime() - deadline < 0, "Request was never offered to the log");
        Thread.onSpinWait();
      }
    }

    void awaitAccepted(int count) throws IOException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      while (log.accepted.size() < count) {
        int work = agent.doWork();
        assertTrue(System.nanoTime() - deadline < 0, "Requests were not accepted by the log");
        idle.idle(work);
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
      responsePublications.close();
      requests.close();
      aeron.close();
      driver.close();
    }
  }
}
