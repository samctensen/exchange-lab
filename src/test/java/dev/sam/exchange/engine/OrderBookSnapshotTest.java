package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

class OrderBookSnapshotTest {

  @Test
  void returnsEmptySnapshotForEmptyBook() {
    OrderBook orderBook = new OrderBook();

    assertEquals(List.of(), orderBook.snapshot());
  }

  @Test
  void capturesOriginalOrdersAndRemainingLotsInArrivalOrder() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder first = new PlaceOrder(20L, Side.ASK, 101L, 5L);
    PlaceOrder second = new PlaceOrder(10L, Side.BID, 99L, 4L);
    PlaceOrder third = new PlaceOrder(30L, Side.ASK, 100L, 7L);
    orderBook.add(first);
    orderBook.add(second);
    orderBook.add(third);
    orderBook.fill(20L, 2L);

    assertEquals(List.of(new OrderSnapshot(first, 3L), new OrderSnapshot(second, 4L), new OrderSnapshot(third, 7L)),
        orderBook.snapshot());
  }

  @Test
  void retainsCapturedStateAfterBookChanges() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder first = new PlaceOrder(20L, Side.ASK, 100L, 5L);
    PlaceOrder second = new PlaceOrder(10L, Side.ASK, 101L, 2L);
    PlaceOrder third = new PlaceOrder(30L, Side.BID, 99L, 4L);
    orderBook.add(first);
    orderBook.add(second);
    orderBook.fill(20L, 2L);
    List<OrderSnapshot> snapshot = orderBook.snapshot();

    orderBook.fill(20L, 1L);
    orderBook.cancel(10L);
    orderBook.add(third);

    assertEquals(List.of(new OrderSnapshot(first, 3L), new OrderSnapshot(second, 2L)), snapshot);
    assertEquals(List.of(new OrderSnapshot(first, 2L), new OrderSnapshot(third, 4L)), orderBook.snapshot());
  }

  @Test
  void doesNotAllowSnapshotListToBeModified() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    orderBook.add(order);
    List<OrderSnapshot> snapshot = orderBook.snapshot();

    assertThrows(UnsupportedOperationException.class, () -> snapshot.clear());
    assertEquals(List.of(new OrderSnapshot(order, 5L)), snapshot);
    assertEquals(snapshot, orderBook.snapshot());
  }
}
