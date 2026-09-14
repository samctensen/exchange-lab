package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PlaceOrderTest {

  @Test
  void rejectsNegativeQuantity() {
    assertThrows(IllegalArgumentException.class, () -> new PlaceOrder(1L, Side.BID, 101L, -5L));
  }

  @Test
  void rejectsZeroQuantity() {
    assertThrows(IllegalArgumentException.class, () -> new PlaceOrder(1L, Side.BID, 101L, 0L));
  }

  @Test
  void acceptsPositiveQuantity() {
    assertEquals(5L, new PlaceOrder(1L, Side.BID, 101L, 5L).quantityLots());
  }

  @Test
  void rejectsNegativePrice() {
    assertThrows(IllegalArgumentException.class, () -> new PlaceOrder(1L, Side.BID, -1L, 5L));
  }

  @Test
  void rejectsZeroPrice() {
    assertThrows(IllegalArgumentException.class, () -> new PlaceOrder(1L, Side.BID, 0L, 5L));
  }

  @Test
  void rejectsMissingSide() {
    assertThrows(IllegalArgumentException.class, () -> new PlaceOrder(1L, null, 5L, 5L));
  }
}
