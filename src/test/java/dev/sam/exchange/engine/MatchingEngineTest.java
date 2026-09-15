package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class MatchingEngineTest {

  @Test
  void validPartialFillRestingAskOrder() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder restingAskOrder = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    orderBook.add(restingAskOrder);
    assertEquals(1, orderBook.size());

    long incomingBidOrderId = 2L;
    PlaceOrder incomingBidOrder = new PlaceOrder(incomingBidOrderId, Side.BID, 105L, 2L);
    orderBook.add(incomingBidOrder);
    assertEquals(2, orderBook.size());

    MatchingEngine engine = new MatchingEngine(orderBook);
    Trade expectedTrade = new Trade(2L, 1L, 100L, 2L);

    assertEquals(Optional.of(expectedTrade), engine.matchOnce(incomingBidOrderId));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(1L));
    assertEquals(Optional.empty(), orderBook.find(2L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void validPartialFillRestingBidOrder() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder restingBidOrder = new PlaceOrder(1L, Side.BID, 105L, 5L);
    orderBook.add(restingBidOrder);
    assertEquals(1, orderBook.size());

    long incomingAskOrderId = 2L;
    PlaceOrder incomingAskOrder = new PlaceOrder(incomingAskOrderId, Side.ASK, 100L, 2L);
    orderBook.add(incomingAskOrder);
    assertEquals(2, orderBook.size());

    MatchingEngine engine = new MatchingEngine(orderBook);
    Trade expectedTrade = new Trade(2L, 1L, 105L, 2L);

    assertEquals(Optional.of(expectedTrade), engine.matchOnce(incomingAskOrderId));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(1L));
    assertEquals(Optional.empty(), orderBook.find(2L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void matchesRemainingQuantityOfPartiallyFilledRestingOrder() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder restingAskOrder = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    orderBook.add(restingAskOrder);
    orderBook.fill(1L, 3L);
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(1L));

    PlaceOrder incomingBidOrder = new PlaceOrder(2L, Side.BID, 105L, 4L);
    orderBook.add(incomingBidOrder);
    MatchingEngine engine = new MatchingEngine(orderBook);

    Trade expectedTrade = new Trade(2L, 1L, 100L, 2L);
    assertEquals(Optional.of(expectedTrade), engine.matchOnce(2L));
    assertEquals(Optional.empty(), orderBook.find(1L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(2L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void matchesOnEqualPricing() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder restingAskOrder = new PlaceOrder(1L, Side.ASK, 100L, 2L);
    orderBook.add(restingAskOrder);
    assertEquals(1, orderBook.size());

    PlaceOrder incomingBidOrder = new PlaceOrder(2L, Side.BID, 100L, 2L);
    orderBook.add(incomingBidOrder);
    assertEquals(2, orderBook.size());
    MatchingEngine engine = new MatchingEngine(orderBook);

    Trade expectedTrade = new Trade(2L, 1L, 100L, 2L);
    assertEquals(Optional.of(expectedTrade), engine.matchOnce(2L));
    assertEquals(Optional.empty(), orderBook.find(1L));
    assertEquals(Optional.empty(), orderBook.find(2L));
    assertEquals(0, orderBook.size());
  }

  @Test
  void doesNotMatchWhenPricesDoNotCross() {
    OrderBook orderBook = new OrderBook();
    long restingAskOrderId = 1L;
    PlaceOrder restingAskOrder = new PlaceOrder(restingAskOrderId, Side.ASK, 105L, 5L);
    orderBook.add(restingAskOrder);
    assertEquals(1, orderBook.size());

    long incomingBidOrderId = 2L;
    PlaceOrder incomingBidOrder = new PlaceOrder(incomingBidOrderId, Side.BID, 100L, 2L);
    orderBook.add(incomingBidOrder);
    assertEquals(2, orderBook.size());

    MatchingEngine engine = new MatchingEngine(orderBook);

    assertEquals(Optional.empty(), engine.matchOnce(incomingBidOrderId));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(restingAskOrderId));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(incomingBidOrderId));
    assertEquals(2, orderBook.size());
  }

  @ParameterizedTest
  @CsvSource({"BID, ASK", "ASK, BID"})
  void matchesEarliestOrderAtTheSamePrice(Side incomingSide, Side restingSide) {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(20L, restingSide, 100L, 2L));
    orderBook.add(new PlaceOrder(10L, restingSide, 100L, 3L));
    orderBook.add(new PlaceOrder(30L, incomingSide, 100L, 4L));
    MatchingEngine engine = new MatchingEngine(orderBook);

    Trade expectedTrade = new Trade(30L, 20L, 100L, 2L);
    assertEquals(Optional.of(expectedTrade), engine.matchOnce(30L));
    assertEquals(Optional.empty(), orderBook.find(20L));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(10L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(30L));
    assertEquals(2, orderBook.size());
  }

  @ParameterizedTest
  @CsvSource({"BID, ASK, 101, 100, 105", "ASK, BID, 100, 101, 95"})
  void matchesBetterPriceBeforeEarlierArrival(Side incomingSide, Side restingSide, long earlierPrice, long betterPrice,
      long incomingPrice) {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(20L, restingSide, earlierPrice, 5L));
    orderBook.add(new PlaceOrder(10L, restingSide, betterPrice, 5L));
    orderBook.add(new PlaceOrder(30L, incomingSide, incomingPrice, 2L));
    MatchingEngine engine = new MatchingEngine(orderBook);

    Trade expectedTrade = new Trade(30L, 10L, betterPrice, 2L);
    assertEquals(Optional.of(expectedTrade), engine.matchOnce(30L));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(20L));
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(10L));
    assertEquals(Optional.empty(), orderBook.find(30L));
    assertEquals(2, orderBook.size());
  }

  @ParameterizedTest
  @EnumSource(Side.class)
  void doesNotMatchWhenOppositeSideIsEmpty(Side side) {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(1L, side, 100L, 5L));
    orderBook.add(new PlaceOrder(2L, side, 100L, 2L));
    MatchingEngine engine = new MatchingEngine(orderBook);

    assertEquals(Optional.empty(), engine.matchOnce(2L));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(1L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(2L));
    assertEquals(2, orderBook.size());
  }

  @Test
  void doesNotMatchIncomingAskWhenPricesDoNotCross() {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(1L, Side.BID, 100L, 5L));
    orderBook.add(new PlaceOrder(2L, Side.ASK, 105L, 2L));
    MatchingEngine engine = new MatchingEngine(orderBook);

    assertEquals(Optional.empty(), engine.matchOnce(2L));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(1L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(2L));
    assertEquals(2, orderBook.size());
  }

  @Test
  void rejectsUnknownIncomingOrderId() {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(1L, Side.ASK, 100L, 5L));
    MatchingEngine engine = new MatchingEngine(orderBook);

    assertThrows(IllegalArgumentException.class, () -> engine.matchOnce(99L));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(1L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void matchesRemainingQuantityOfPartiallyFilledIncomingOrder() {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(1L, Side.ASK, 100L, 4L));
    orderBook.add(new PlaceOrder(2L, Side.BID, 105L, 5L));
    orderBook.fill(2L, 3L);
    MatchingEngine engine = new MatchingEngine(orderBook);

    Trade expectedTrade = new Trade(2L, 1L, 100L, 2L);
    assertEquals(Optional.of(expectedTrade), engine.matchOnce(2L));
    assertEquals(OptionalLong.of(2L), orderBook.remainingLots(1L));
    assertEquals(Optional.empty(), orderBook.find(2L));
    assertEquals(1, orderBook.size());
  }

  @Test
  void matchMultiple() {
    OrderBook orderBook = new OrderBook();
    orderBook.add(new PlaceOrder(20L, Side.ASK, 100L, 2L));
    orderBook.add(new PlaceOrder(10L, Side.ASK, 101L, 3L));
    orderBook.add(new PlaceOrder(30L, Side.BID, 102L, 4L));
    assertEquals(3, orderBook.size());

    MatchingEngine engine = new MatchingEngine(orderBook);
    List<Trade> matches = engine.match(30L);

    assertEquals(2, matches.size());
    Trade expectedTrade1 = new Trade(30L, 20L, 100L, 2L);
    Trade expectedTrade2 = new Trade(30L, 10L, 101L, 2L);
    assertEquals(expectedTrade1, matches.getFirst());
    assertEquals(expectedTrade2, matches.getLast());
    assertEquals(1, orderBook.size());
    assertEquals(OptionalLong.of(1L), orderBook.remainingLots(10L));
  }
}
