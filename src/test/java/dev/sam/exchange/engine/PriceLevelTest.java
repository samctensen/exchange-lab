package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class PriceLevelTest {
  @Test
  void rejectsNullSide() {
    assertThrows(NullPointerException.class, () -> new PriceLevel(null, 100L));
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
  void rejectsNonPositivePrice(long priceTicks) {
    assertThrows(IllegalArgumentException.class, () -> new PriceLevel(Side.ASK, priceTicks));
  }

  @ParameterizedTest
  @EnumSource(Side.class)
  void returnsEmptyUntilAnOrderIsAdded(Side side) {
    PriceLevel level = new PriceLevel(side, 100L);
    assertTrue(level.isEmpty());
    assertTrue(level.first().isEmpty());

    RestingOrder order = new RestingOrder(new PlaceOrder(20L, side, 100L, 5L));
    level.add(order);

    assertFalse(level.isEmpty());
    assertSame(order, level.first().orElseThrow());
  }

  @Test
  void peekingAndPartialFillingPreserveArrivalPriorityAndObjectIdentity() {
    PriceLevel level = new PriceLevel(Side.ASK, 100L);
    RestingOrder first = ask(20L);
    RestingOrder second = ask(10L);
    level.add(first);
    level.add(second);

    assertSame(first, level.first().orElseThrow());
    level.first().orElseThrow().fill(2L);

    assertSame(first, level.first().orElseThrow());
    assertEquals(3L, first.remainingLots());
    assertEquals(5L, second.remainingLots());
  }

  @ParameterizedTest
  @CsvSource({"20, 10, 30", "10, 20, 30", "30, 20, 10"})
  void removingHeadMiddleOrTailPreservesSurvivorOrder(long removedId, long nextId, long lastId) {
    PriceLevel level = new PriceLevel(Side.ASK, 100L);
    level.add(ask(20L));
    level.add(ask(10L));
    level.add(ask(30L));

    assertEquals(removedId, level.remove(removedId).orElseThrow().order().orderId());
    assertTrue(level.remove(removedId).isEmpty());
    assertEquals(nextId, level.first().orElseThrow().order().orderId());
    level.remove(nextId);
    assertEquals(lastId, level.first().orElseThrow().order().orderId());
    level.remove(lastId);

    assertTrue(level.first().isEmpty());
    assertTrue(level.isEmpty());
  }

  @Test
  void removingUnknownIdPreservesExistingOrder() {
    PriceLevel level = new PriceLevel(Side.ASK, 100L);
    RestingOrder order = ask(20L);
    level.add(order);

    assertTrue(level.remove(999L).isEmpty());
    assertSame(order, level.first().orElseThrow());
    assertSame(order, level.remove(20L).orElseThrow());
    assertTrue(level.isEmpty());
  }

  @ParameterizedTest
  @CsvSource({"BID, 100", "ASK, 101"})
  void rejectingMismatchedOrderLeavesLevelUnchanged(Side side, long priceTicks) {
    PriceLevel level = new PriceLevel(Side.ASK, 100L);
    RestingOrder original = ask(20L);
    level.add(original);
    RestingOrder invalid = new RestingOrder(new PlaceOrder(40L, side, priceTicks, 5L));

    assertThrows(IllegalArgumentException.class, () -> level.add(invalid));

    assertSame(original, level.first().orElseThrow());
    level.remove(20L);
    assertTrue(level.isEmpty());
  }

  @Test
  void rejectingDuplicateIdPreservesOriginalOrderAndPriority() {
    PriceLevel level = new PriceLevel(Side.ASK, 100L);
    RestingOrder first = ask(20L);
    RestingOrder second = ask(10L);
    level.add(first);
    level.add(second);
    RestingOrder duplicate = new RestingOrder(new PlaceOrder(20L, Side.ASK, 100L, 99L));

    assertThrows(IllegalArgumentException.class, () -> level.add(duplicate));

    assertSame(first, level.first().orElseThrow());
    assertEquals(5L, first.remainingLots());
    assertSame(first, level.remove(20L).orElseThrow());
    assertSame(second, level.first().orElseThrow());
    level.remove(10L);
    assertTrue(level.isEmpty());
  }

  private static RestingOrder ask(long orderId) {
    return new RestingOrder(new PlaceOrder(orderId, Side.ASK, 100L, 5L));
  }
}
