package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OrderSnapshotTest {
  private final PlaceOrder order = new PlaceOrder(1, Side.BID, 100, 10);

  @Test
  void requiresAnOrder() {
    assertThrows(NullPointerException.class, () -> new OrderSnapshot(null, 1));
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MIN_VALUE, -1, 0, 11, Long.MAX_VALUE})
  void onlyRepresentsRemainingQuantityOfALiveOrder(long remaining) {
    assertThrows(IllegalArgumentException.class, () -> new OrderSnapshot(order, remaining));
  }

  @ParameterizedTest
  @ValueSource(longs = {1, 10})
  void acceptsPartialAndUnfilledOrders(long remaining) {
    OrderSnapshot snapshot = new OrderSnapshot(order, remaining);
    assertEquals(order, snapshot.order());
    assertEquals(remaining, snapshot.remainingLots());
  }
}
