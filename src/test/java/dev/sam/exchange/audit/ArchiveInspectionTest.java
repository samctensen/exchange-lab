package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.BookDepth;
import dev.sam.exchange.engine.BookSummary;
import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PriceLevelSnapshot;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;

@Timeout(30)
class ArchiveInspectionTest {
  @TempDir
  Path directory;

  @Test
  void replayInspectsStoppedHistoryWithoutAppendingOrChangingItsStopPosition() throws Exception {
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"),
        directory.resolve("driver").toString())) {
      long id;
      try (ArchiveRequestLog log = ArchiveRequestLog.open(runtime.archive(),
          ignored -> fail("Expected empty history"))) {
        id = log.recordingId();
        append(log, history());
      }
      long stop = runtime.archive().getStopPosition(id);
      ArchiveInspection inspection = ArchiveInspector.inspect(runtime.archive(), 1);
      assertEquals(inspection, ArchiveInspector.inspect(runtime.archive(), 1));
      assertEquals(stop, runtime.archive().getStopPosition(id));
      assertHistory(inspection);
      var book = new OrderBook();
      var state = new RequestStateMachine(new MatchingEngine(book));
      try (ArchiveRequestLog resumed = ArchiveRequestLog.open(runtime.archive(), state::process)) {
        assertEquals(id, resumed.recordingId());
        assertEquals(new BigInteger("9"), book.summary().bidLots());
        append(resumed, List.of(request(8, new CancelOrder(10))));
      }
      var extended = ArchiveInspector.inspect(runtime.archive(), 1);
      assertEquals(new BigInteger("3"), extended.book().bidLots());
      assertEquals(10, extended.audit().observedRequests());
      assertNotEquals(inspection.audit().historySha256(), extended.audit().historySha256());
    }
  }

  @Test
  void emptyCatalogDoesNotCreateAnAuthoritativeRequestRecording() throws Exception {
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"),
        directory.resolve("driver").toString())) {
      var empty = ArchiveInspector.inspect(runtime.archive(), 10);
      assertEquals(0, empty.audit().observedRequests());
      assertEquals(new BookDepth(List.of(), List.of()), empty.depth());
      assertEquals("b2f9064c93325a748e6dbbe28931d844f9c50e1612a109251001f02f45e0ec5c", empty.audit().historySha256());
      assertEquals(0, runtime.archive().listRecordings(0, 10,
          (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) -> fail("Inspector created a recording")));
    }
  }

  @Test
  void invalidDepthAndInterruptionFailBeforeCatalogueIo() throws Exception {
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"),
        directory.resolve("driver").toString())) {
      long before = runtime.archive().lastCorrelationId();
      assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(runtime.archive(), 0));
      assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(runtime.archive(), 1001));
      Thread.currentThread().interrupt();
      try {
        assertThrows(IllegalStateException.class, () -> ArchiveInspector.inspect(runtime.archive(), 10));
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }
      assertEquals(before, runtime.archive().lastCorrelationId());
    }
  }

  static void assertHistory(ArchiveInspection inspection) {
    var a = inspection.audit();
    assertEquals(9, a.observedRequests());
    assertEquals(7, a.uniqueRequests());
    assertEquals(1, a.duplicateRequests());
    assertEquals(1, a.conflictingRequests());
    assertEquals(6, a.placeRequests());
    assertEquals(1, a.cancelRequests());
    assertEquals(1, a.businessRejections());
    assertEquals(1, a.trades());
    assertEquals(BigInteger.valueOf(4), a.executedLots());
    assertEquals(BigInteger.valueOf(400), a.notionalTickLots());
    assertEquals(new BookSummary(2, 2, 2, 2, BigInteger.valueOf(9), BigInteger.valueOf(9)), inspection.book());
    assertEquals(new BookDepth(List.of(new PriceLevelSnapshot(100, 1, BigInteger.valueOf(6))),
        List.of(new PriceLevelSnapshot(105, 1, BigInteger.valueOf(7)))), inspection.depth());
  }

  static List<CommandRequest> history() {
    var ask = request(2, new PlaceOrder(20, Side.ASK, 99, 4));
    return List.of(request(1, new PlaceOrder(10, Side.BID, 100, 10)), ask, ask,
        request(3, new PlaceOrder(10, Side.BID, 100, 1)), request(1, new CancelOrder(10)),
        request(4, new CancelOrder(999)), request(5, new PlaceOrder(30, Side.ASK, 105, 7)),
        request(6, new PlaceOrder(40, Side.ASK, 106, 2)), request(7, new PlaceOrder(50, Side.BID, 98, 3)));
  }

  static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0, id), command);
  }

  static void append(ArchiveRequestLog log, List<CommandRequest> requests) throws Exception {
    long[] position = {-1};
    for (var request : requests)
      await(() -> (position[0] = log.offer(request)) >= 0);
    if (!requests.isEmpty())
      await(() -> log.isRecorded(position[0]));
  }

  static void await(BooleanSupplier done) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!done.getAsBoolean()) {
      if (System.nanoTime() - deadline >= 0)
        fail("Archive operation timed out");
      Thread.sleep(1);
    }
  }
}
