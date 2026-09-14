package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RestingOrderTest {

  @Test
  void returnsFullQuantityNewRestingOrder() {
    long quantityLots = 5L;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    assertEquals(quantityLots, new RestingOrder(order).remainingLots());
  }

  @Test
  void returnsRemainingQuantityAfterPartialFill() {
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, 5L);
    RestingOrder restingOrder = new RestingOrder(order);
    long quantityLots = 2L;
    restingOrder.fill(quantityLots);
    assertEquals(3L, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }

  @Test
  void returnsZeroQuantityAfterFill() {
    long quantityLots = 5L;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    RestingOrder restingOrder = new RestingOrder(order);
    restingOrder.fill(quantityLots);
    assertEquals(0L, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }

  @Test
  void rejectsNegativeQuantityLots() {
    long quantityLots = 5l;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    RestingOrder restingOrder = new RestingOrder(order);
    long fillLots = -1L;
    assertThrows(IllegalArgumentException.class, () -> restingOrder.fill(fillLots));
    assertEquals(quantityLots, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }

  @Test
  void rejectsZeroQuantityLots() {
    long quantityLots = 5l;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    RestingOrder restingOrder = new RestingOrder(order);
    long fillLots = 0L;
    assertThrows(IllegalArgumentException.class, () -> restingOrder.fill(fillLots));
    assertEquals(quantityLots, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }

  @Test
  void rejectsExceedingQuantityLots() {
    long quantityLots = 5L;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    RestingOrder restingOrder = new RestingOrder(order);
    long fillLots = 6L;
    assertThrows(IllegalArgumentException.class, () -> restingOrder.fill(fillLots));
    assertEquals(quantityLots, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }

  @Test
  void rejectsOverfillAfterPartialQuantityLots() {
    long quantityLots = 5l;
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 101L, quantityLots);
    RestingOrder restingOrder = new RestingOrder(order);
    long fillLots1 = 2L;
    restingOrder.fill(fillLots1);
    assertEquals(3L, restingOrder.remainingLots());
    long fillLots2 = 4L;
    assertThrows(IllegalArgumentException.class, () -> restingOrder.fill(fillLots2));
    assertEquals(3L, restingOrder.remainingLots());
    assertEquals(order, restingOrder.order());
  }
}
