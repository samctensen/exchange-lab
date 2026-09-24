package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OrderBookPriceLevelTest {
  @ParameterizedTest
  @EnumSource(Side.class)
  void cancellingAndFillingExhaustsOneLevelBeforeAdvancingToTheNext(Side side) {
    OrderBook book = new OrderBook();
    PlaceOrder first = new PlaceOrder(20L, side, 100L, 5L);
    PlaceOrder middle = new PlaceOrder(10L, side, 100L, 5L);
    PlaceOrder last = new PlaceOrder(30L, side, 100L, 5L);
    PlaceOrder nextLevel = new PlaceOrder(40L, side, side == Side.BID ? 99L : 101L, 5L);
    book.add(nextLevel);
    book.add(first);
    book.add(middle);
    book.add(last);

    assertBest(book, side, first);
    assertEquals(Optional.of(middle), book.cancel(10L));
    assertBest(book, side, first);
    book.fill(20L, 2L);
    assertEquals(OptionalLong.of(3L), book.remainingLots(20L));
    assertBest(book, side, first);
    book.fill(20L, 3L);
    assertTrue(book.find(20L).isEmpty());
    assertBest(book, side, last);
    book.cancel(30L);
    assertBest(book, side, nextLevel);
    assertEquals(1, book.size());
    book.fill(40L, 5L);

    assertEmpty(book, side);
    assertEquals(0, book.size());
    assertTrue(book.snapshot().isEmpty());
  }

  @ParameterizedTest
  @EnumSource(Side.class)
  void reusedIdJoinsTheBackOfItsLevelAndAnExhaustedLevelCanBeRecreated(Side side) {
    OrderBook book = new OrderBook();
    PlaceOrder first = new PlaceOrder(20L, side, 100L, 5L);
    PlaceOrder second = new PlaceOrder(10L, side, 100L, 5L);
    PlaceOrder replacement = new PlaceOrder(20L, side, 100L, 7L);
    book.add(first);
    book.add(second);
    book.cancel(20L);
    book.add(replacement);

    assertBest(book, side, second);
    assertEquals(List.of(new OrderSnapshot(second, 5L), new OrderSnapshot(replacement, 7L)), book.snapshot());
    book.fill(10L, 5L);
    assertBest(book, side, replacement);
    book.fill(20L, 7L);
    assertEmpty(book, side);

    book.add(first);
    assertBest(book, side, first);
    assertEquals(OptionalLong.of(5L), book.remainingLots(20L));
    assertEquals(1, book.size());
    book.cancel(20L);
    assertEmpty(book, side);
  }

  @ParameterizedTest
  @EnumSource(Side.class)
  void cancelledIdCanMoveToTheOppositeSideWithoutChangingThatSidesExistingPriority(Side side) {
    OrderBook book = new OrderBook();
    Side opposite = side == Side.BID ? Side.ASK : Side.BID;
    PlaceOrder original = new PlaceOrder(20L, side, 100L, 5L);
    PlaceOrder existingOpposite = new PlaceOrder(10L, opposite, 101L, 5L);
    PlaceOrder replacement = new PlaceOrder(20L, opposite, 101L, 7L);
    book.add(original);
    book.add(existingOpposite);
    book.cancel(20L);
    book.add(replacement);

    assertEmpty(book, side);
    assertBest(book, opposite, existingOpposite);
    assertEquals(Optional.of(replacement), book.find(20L));
    book.fill(10L, 5L);
    assertBest(book, opposite, replacement);
    book.cancel(20L);
    assertEmpty(book, opposite);
    assertEquals(0, book.size());
  }

  private static void assertBest(OrderBook book, Side side, PlaceOrder expected) {
    Optional<PlaceOrder> order = side == Side.BID ? book.bestBid() : book.bestAsk();
    OptionalLong price = side == Side.BID ? book.bestBidPrice() : book.bestAskPrice();
    assertEquals(Optional.of(expected), order);
    assertEquals(OptionalLong.of(expected.priceTicks()), price);
  }

  private static void assertEmpty(OrderBook book, Side side) {
    Optional<PlaceOrder> order = side == Side.BID ? book.bestBid() : book.bestAsk();
    OptionalLong price = side == Side.BID ? book.bestBidPrice() : book.bestAskPrice();
    assertTrue(order.isEmpty());
    assertTrue(price.isEmpty());
  }
}
