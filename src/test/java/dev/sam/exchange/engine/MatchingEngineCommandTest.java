package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class MatchingEngineCommandTest {

  @ParameterizedTest
  @EnumSource(Side.class)
  void reportsRemainingQuantityWhenOrderRests(Side side) {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    PlaceOrder order = new PlaceOrder(7L, side, 100L, 2L);

    assertEquals(new PlaceResult(7L, List.of(), 2L), engine.process(order));
    assertEquals(Optional.of(order), orderBook.find(7L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(7L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void reportsTradesInExecutionOrderAndZeroRemainingForFullyFilledOrder() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    engine.process(new PlaceOrder(20L, Side.ASK, 100L, 2L));
    engine.process(new PlaceOrder(10L, Side.ASK, 101L, 3L));

    List<Trade> expectedTrades = List.of(new Trade(30L, 20L, 100L, 2L), new Trade(30L, 10L, 101L, 2L));
    assertEquals(new PlaceResult(30L, expectedTrades, 0L), engine.process(new PlaceOrder(30L, Side.BID, 102L, 4L)));
    assertEquals(Optional.empty(), orderBook.find(20L));
    assertEquals(Optional.empty(), orderBook.find(30L));
    assertEquals(OptionalLong.of(1L), orderBook.remainingLots(10L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void reportsRemainingQuantityAfterOppositeSideIsExhausted() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    engine.process(new PlaceOrder(1L, Side.BID, 105L, 2L));

    assertEquals(new PlaceResult(2L, List.of(new Trade(2L, 1L, 105L, 2L)), 3L),
        engine.process(new PlaceOrder(2L, Side.ASK, 100L, 5L)));
    assertEquals(Optional.empty(), orderBook.find(1L));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(2L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void reportsRemainingQuantityWhenNextPriceDoesNotCross() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    engine.process(new PlaceOrder(1L, Side.ASK, 100L, 2L));
    engine.process(new PlaceOrder(2L, Side.ASK, 105L, 3L));

    assertEquals(new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 2L)), 2L),
        engine.process(new PlaceOrder(3L, Side.BID, 102L, 4L)));
    assertEquals(Optional.empty(), orderBook.find(1L));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(2L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(3L));
    assertEquals(2, orderBook.size());
  }

  @Test
  void reportsWhetherCancellationRemovedAnOrder() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    engine.process(new PlaceOrder(7L, Side.BID, 100L, 2L));

    assertEquals(new CancelResult(7L, true), engine.process(new CancelOrder(7L)));
    assertEquals(Optional.empty(), orderBook.find(7L));
    assertEquals(OptionalLong.empty(), orderBook.remainingLots(7L));
    assertEquals(0, orderBook.size());
    assertEquals(new CancelResult(7L, false), engine.process(new CancelOrder(7L)));
    assertEquals(0, orderBook.size());
  }

  @Test
  void cancellingUnknownOrderLeavesOtherOrdersIntact() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    PlaceOrder existingOrder = new PlaceOrder(7L, Side.BID, 100L, 2L);
    engine.process(existingOrder);

    assertEquals(new CancelResult(99L, false), engine.process(new CancelOrder(99L)));
    assertEquals(Optional.of(existingOrder), orderBook.find(7L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(7L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void rejectsDuplicateActiveIdWithoutChangingTheBook() {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    PlaceOrder existingBid = new PlaceOrder(7L, Side.BID, 100L, 2L);
    PlaceOrder existingAsk = new PlaceOrder(8L, Side.ASK, 105L, 3L);
    engine.process(existingBid);
    engine.process(existingAsk);

    assertThrows(IllegalArgumentException.class, () -> engine.process(new PlaceOrder(7L, Side.BID, 110L, 5L)));
    assertEquals(Optional.of(existingBid), orderBook.find(7L));
    assertEquals(Optional.of(existingAsk), orderBook.find(8L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(7L));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(8L));
    assertEquals(2, orderBook.size());
  }
}
