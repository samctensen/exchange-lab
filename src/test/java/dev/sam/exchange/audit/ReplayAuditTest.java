package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;

class ReplayAuditTest {
  private final OrderBook book = new OrderBook();
  private final ReplayAudit audit = new ReplayAudit(new RequestStateMachine(new MatchingEngine(book)));

  @Test
  void freshAuditHasNoObservedOrExecutedWork() {
    ReplayAuditSnapshot snapshot = audit.snapshot();

    assertEquals(0L, snapshot.observedRequests());
    assertEquals(0L, snapshot.uniqueRequests());
    assertEquals(0L, snapshot.duplicateRequests());
    assertEquals(0L, snapshot.conflictingRequests());
    assertEquals(0L, snapshot.placeRequests());
    assertEquals(0L, snapshot.cancelRequests());
    assertEquals(0L, snapshot.businessRejections());
    assertEquals(0L, snapshot.trades());
    assertEquals(BigInteger.ZERO, snapshot.executedLots());
    assertEquals(BigInteger.ZERO, snapshot.notionalTickLots());
  }

  @Test
  void appliesUniqueCommandsAndSeparatesBusinessRejectionsFromMissingCancellations() {
    PlaceOrder survivor = new PlaceOrder(20L, Side.ASK, 110L, 7L);
    audit.accept(request(1L, new PlaceOrder(10L, Side.BID, 100L, 5L)));
    audit.accept(request(2L, new PlaceOrder(10L, Side.BID, 99L, 2L)));
    audit.accept(request(3L, new CancelOrder(10L)));
    audit.accept(request(4L, new CancelOrder(10L)));
    audit.accept(request(5L, survivor));

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(5L, snapshot.observedRequests());
    assertEquals(5L, snapshot.uniqueRequests());
    assertEquals(3L, snapshot.placeRequests());
    assertEquals(2L, snapshot.cancelRequests());
    assertEquals(1L, snapshot.businessRejections());
    assertEquals(List.of(new OrderSnapshot(survivor, 7L)), book.snapshot());
  }

  @Test
  void countsRepeatedAndConflictingUuidsAsObservationsWithoutNewExecutions() {
    PlaceOrder order = new PlaceOrder(10L, Side.BID, 100L, 5L);
    CommandRequest original = request(1L, order);
    CommandRequest conflict = request(1L, new CancelOrder(10L));
    audit.accept(original);
    audit.accept(request(1L, new PlaceOrder(10L, Side.BID, 100L, 5L)));
    audit.accept(conflict);
    audit.accept(original);
    audit.accept(conflict);

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(5L, snapshot.observedRequests());
    assertEquals(1L, snapshot.uniqueRequests());
    assertEquals(2L, snapshot.duplicateRequests());
    assertEquals(2L, snapshot.conflictingRequests());
    assertEquals(1L, snapshot.placeRequests());
    assertEquals(0L, snapshot.cancelRequests());
    assertEquals(0L, snapshot.businessRejections());
    assertEquals(List.of(new OrderSnapshot(order, 5L)), book.snapshot());
  }

  @Test
  void cachedBusinessRejectionsAndCancellationsDoNotCountAsFreshExecutions() {
    CommandRequest rejected = request(2L, new PlaceOrder(10L, Side.ASK, 101L, 3L));
    CommandRequest cancel = request(3L, new CancelOrder(10L));
    PlaceOrder replacement = new PlaceOrder(10L, Side.ASK, 102L, 8L);
    audit.accept(request(1L, new PlaceOrder(10L, Side.BID, 100L, 5L)));
    audit.accept(rejected);
    audit.accept(cancel);
    audit.accept(rejected);
    audit.accept(request(4L, replacement));
    audit.accept(cancel);

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(6L, snapshot.observedRequests());
    assertEquals(4L, snapshot.uniqueRequests());
    assertEquals(2L, snapshot.duplicateRequests());
    assertEquals(0L, snapshot.conflictingRequests());
    assertEquals(3L, snapshot.placeRequests());
    assertEquals(1L, snapshot.cancelRequests());
    assertEquals(1L, snapshot.businessRejections());
    assertEquals(List.of(new OrderSnapshot(replacement, 8L)), book.snapshot());
  }

  @Test
  void countsOnlyFreshTradesAtTheirRestingPricesAcrossMultipleLevels() {
    CommandRequest bid = request(3L, new PlaceOrder(30L, Side.BID, 105L, 4L));
    CommandRequest ask = request(5L, new PlaceOrder(50L, Side.ASK, 98L, 2L));
    audit.accept(request(1L, new PlaceOrder(10L, Side.ASK, 100L, 2L)));
    audit.accept(request(2L, new PlaceOrder(20L, Side.ASK, 102L, 3L)));
    audit.accept(bid);
    audit.accept(request(4L, new PlaceOrder(40L, Side.BID, 99L, 6L)));
    audit.accept(ask);
    audit.accept(bid);
    audit.accept(ask);
    audit.accept(request(6L, new PlaceOrder(20L, Side.BID, 200L, 5L)));
    audit.accept(request(3L, new PlaceOrder(30L, Side.BID, 106L, 4L)));

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(9L, snapshot.observedRequests());
    assertEquals(6L, snapshot.uniqueRequests());
    assertEquals(2L, snapshot.duplicateRequests());
    assertEquals(1L, snapshot.conflictingRequests());
    assertEquals(1L, snapshot.businessRejections());
    // Two lots at 100, two at 102 and two at 99: 3 trades, 6 lots, 602 tick-lots.
    assertEquals(3L, snapshot.trades());
    assertEquals(new BigInteger("6"), snapshot.executedLots());
    assertEquals(new BigInteger("602"), snapshot.notionalTickLots());
    assertEquals(1L, book.remainingLots(20L).orElseThrow());
    assertEquals(4L, book.remainingLots(40L).orElseThrow());
  }

  @Test
  void preservesExactVolumeAndNotionalBeyondLongRange() {
    audit.accept(request(1L, new PlaceOrder(10L, Side.ASK, Long.MAX_VALUE, Long.MAX_VALUE)));
    audit.accept(request(2L, new PlaceOrder(20L, Side.BID, Long.MAX_VALUE, Long.MAX_VALUE)));
    audit.accept(request(3L, new PlaceOrder(30L, Side.ASK, Long.MAX_VALUE, Long.MAX_VALUE)));
    audit.accept(request(4L, new PlaceOrder(40L, Side.BID, Long.MAX_VALUE, Long.MAX_VALUE)));

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(2L, snapshot.trades());
    assertEquals(new BigInteger("18446744073709551614"), snapshot.executedLots());
    assertEquals(new BigInteger("170141183460469231694793815568465002498"), snapshot.notionalTickLots());
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void fingerprintsEveryObservationIncludingDuplicatesAndConflicts() {
    UUID placeId = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
    CommandRequest place = new CommandRequest(placeId, new PlaceOrder(Long.MIN_VALUE, Side.ASK, 101L, 7L));
    audit.accept(place);
    audit.accept(
        new CommandRequest(UUID.fromString("8899aabb-ccdd-eeff-0011-223344556677"), new CancelOrder(Long.MIN_VALUE)));
    audit.accept(place);
    audit.accept(new CommandRequest(placeId, new PlaceOrder(Long.MIN_VALUE, Side.ASK, 102L, 7L)));

    ReplayAuditSnapshot snapshot = audit.snapshot();
    assertEquals(4L, snapshot.observedRequests());
    assertEquals(2L, snapshot.uniqueRequests());
    assertEquals(1L, snapshot.duplicateRequests());
    assertEquals(1L, snapshot.conflictingRequests());
    assertEquals("229d75b225550185298b7a9523e4c9f8839748bae71cf5e4d9910d456b712930", snapshot.historySha256());
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void snapshotsRemainDetachedWhileLaterRequestsChangeTheAuditAndBook() {
    audit.accept(request(1L, new PlaceOrder(10L, Side.ASK, 100L, 5L)));
    audit.accept(request(2L, new PlaceOrder(20L, Side.BID, 101L, 2L)));
    ReplayAuditSnapshot first = audit.snapshot();
    assertEquals(first, audit.snapshot());

    audit.accept(request(3L, new PlaceOrder(30L, Side.BID, 101L, 1L)));
    ReplayAuditSnapshot later = audit.snapshot();

    assertEquals(2L, first.observedRequests());
    assertEquals(1L, first.trades());
    assertEquals(new BigInteger("2"), first.executedLots());
    assertEquals(new BigInteger("200"), first.notionalTickLots());
    assertEquals(3L, later.observedRequests());
    assertEquals(2L, later.trades());
    assertEquals(new BigInteger("3"), later.executedLots());
    assertEquals(new BigInteger("300"), later.notionalTickLots());
    assertEquals(later, audit.snapshot());
    assertEquals(2L, book.remainingLots(10L).orElseThrow());
  }

  private static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0L, id), command);
  }
}
