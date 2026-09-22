package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.persistence.RequestJournal;
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
      assertEquals(List.of(first), server.journal.readAll());
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
      assertEquals(List.of(first, second), server.journal.readAll());
      assertEquals(List.of(new OrderSnapshot(bid, 6L)), server.book.snapshot());
      assertEquals(0, server.agent.doWork());
      assertFalse(server.requests.isClosed(), "The server still owns the borrowed subscription");
      assertFalse(server.replies.isClosed(), "The server still owns the borrowed publication");
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void countsFragmentsAsWorkButProcessesOnlyTheCompleteRequest(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
      String encoded = new CommandRequestCodec().encode(request).replace(",PLACE,",
          ",PLACE," + "0".repeat(server.commands.maxPayloadLength() * 2));
      assertEquals(request, new CommandRequestCodec().decode(encoded));
      server.sendEncoded(encoded);
      int partialPasses = 0;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();

      while (server.processor.processed.isEmpty()) {
        int work = server.agent.doWork();
        if (work > 0 && server.processor.processed.isEmpty()) {
          partialPasses++;
          assertEquals(List.of(), server.journal.readAll());
          assertEquals(List.of(), server.book.snapshot());
        }
        assertTrue(System.nanoTime() - deadline < 0, "The complete request was never processed");
        idle.idle(work);
      }

      assertTrue(partialPasses > 0, "The fixture must span several polling passes");
      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.journal.readAll());
      assertTrue(server.errors.isEmpty(), server.errors::toString);
    }
  }

  @Test
  void repeatedOffersKeepTheOriginalFiveSecondDeadline(@TempDir Path tempDir) throws Exception {
    try (TestServer server = new TestServer(tempDir)) {
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      server.send(request);
      long started = System.nanoTime();
      server.awaitProcessed(1);
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
      assertTrue(System.nanoTime() - started >= TimeUnit.SECONDS.toNanos(5),
          "The response must retain its full five-second send window");
      assertEquals(List.of(request), server.processor.processed);
      assertEquals(List.of(request), server.journal.readAll());
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
      assertEquals(List.of(request), server.journal.readAll());
      assertTrue(server.errors.isEmpty(), server.errors::toString);
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

  // Keep the real journal and matching engine; record calls so deduplication cannot hide reprocessing.
  private static final class RecordingProcessor extends RequestProcessor {
    private final List<CommandRequest> processed = new ArrayList<>();

    RecordingProcessor(OrderBook book, RequestJournal journal) {
      super(new MatchingEngine(book), journal);
    }

    @Override
    public CommandResponse process(CommandRequest request) throws IOException {
      processed.add(request);
      return super.process(request);
    }
  }

  private static final class TestServer implements AutoCloseable {
    private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private final OrderBook book = new OrderBook();
    private final RequestJournal journal;
    private final RecordingProcessor processor;
    private final MediaDriver driver;
    private final Aeron aeron;
    private final Subscription requests;
    private final Publication replies;
    private final Publication commands;
    private final AeronEngineAgent agent;

    TestServer(Path tempDir) throws IOException {
      journal = new RequestJournal(Files.createFile(tempDir.resolve("requests.journal")));
      processor = new RecordingProcessor(book, journal);
      driver = MediaDriver
          .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
              .ipcMtuLength(256).dirDeleteOnShutdown(true).errorHandler(errors::add));
      aeron = Aeron
          .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
      requests = aeron.addSubscription("aeron:ipc", 1);
      replies = aeron.addPublication("aeron:ipc", 2);
      commands = aeron.addPublication("aeron:ipc", 1);
      agent = new AeronEngineAgent(requests, replies, processor);
      awaitConnected(commands);
    }

    void send(CommandRequest request) {
      sendEncoded(new CommandRequestCodec().encode(request));
    }

    void sendEncoded(String encoded) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
      int length = buffer.putStringAscii(0, encoded);
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
      CommandResponseCodec codec = new CommandResponseCodec();
      FragmentAssembler assembler = new FragmentAssembler(
          (buffer, offset, length, header) -> received.add(codec.decode(buffer.getStringAscii(offset))));
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
