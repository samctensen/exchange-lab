package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

class MatchingEngineTest {

  @Test
  void validPartialFill() {
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
}
