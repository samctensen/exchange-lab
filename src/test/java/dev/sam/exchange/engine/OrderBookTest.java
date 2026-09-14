package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

class OrderBookTest {

  @Test
  void acceptsEmptyOrderbook() {
    OrderBook orderBook = new OrderBook();
    assertEquals(0, orderBook.size());
  }

  @Test
  void acceptsAddingPlaceOrder() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 1, 5L);
    orderBook.add(order);
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
  }

  @Test
  void returnsEmptyForUnknownOrderId() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    assertEquals(Optional.empty(), orderBook.find(placeOrderID));
  }

  @Test
  void rejectsDuplicateOrderId() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 1, 5L);
    orderBook.add(order);
    assertThrows(IllegalArgumentException.class, () -> orderBook.add(new PlaceOrder(placeOrderID, Side.ASK, 1, 5L)));
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
  }

  @Test
  void acceptsAddingMultiplePlaceOrders() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 1L;
    long placeOrderID2 = 2L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 1, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.ASK, 1, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(2, orderBook.size());
    assertEquals(Optional.of(order1), orderBook.find(placeOrderID1));
    assertEquals(Optional.of(order2), orderBook.find(placeOrderID2));
  }

  @Test
  void acceptsCancellingExistingOrder() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 1, 5L);
    orderBook.add(order);
    assertEquals(Optional.of(order), orderBook.cancel(placeOrderID));
    assertEquals(Optional.empty(), orderBook.find(placeOrderID));
    assertEquals(0, orderBook.size());
  }

  @Test
  void acceptsCancellingNonExistingOrder() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 1L;
    long placeOrderID2 = 2L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 1, 5L);
    orderBook.add(order1);
    assertEquals(Optional.empty(), orderBook.cancel(placeOrderID2));
    assertEquals(Optional.of(order1), orderBook.find(placeOrderID1));
    assertEquals(1, orderBook.size());
  }

  @Test
  void acceptsCancellingOneOfMultiplePlaceOrders() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 1L;
    long placeOrderID2 = 2L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 1, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.ASK, 1, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(Optional.of(order1), orderBook.cancel(placeOrderID1));
    assertEquals(Optional.of(order2), orderBook.find(placeOrderID2));
    assertEquals(1, orderBook.size());
  }

  @Test
  void acceptsCancellingSameOrderTwice() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 1, 5L);
    orderBook.add(order);
    assertEquals(Optional.of(order), orderBook.cancel(placeOrderID));
    assertEquals(Optional.empty(), orderBook.cancel(placeOrderID));
  }

  @Test
  void returnsEmptyBestBidWhenNoBidsExist() {
    OrderBook orderBook = new OrderBook();
    assertEquals(OptionalLong.empty(), orderBook.bestBidPrice());
    PlaceOrder order = new PlaceOrder(1L, Side.ASK, 1, 5L);
    orderBook.add(order);
    assertEquals(OptionalLong.empty(), orderBook.bestBidPrice());
  }

  @Test
  void returnsHighestBidPrice() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.BID, 5, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.BID, 15, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.BID, 10, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    assertEquals(OptionalLong.of(15L), orderBook.bestBidPrice());
  }

  @Test
  void ignoresAsksWhenFindingBestBid() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.BID, 5, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.BID, 15, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.BID, 10, 5L);
    PlaceOrder order4 = new PlaceOrder(4L, Side.ASK, 30, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    orderBook.add(order4);
    assertEquals(OptionalLong.of(15L), orderBook.bestBidPrice());
  }

  @Test
  void returnsNextBestBidAfterCancellation() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.BID, 5, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.BID, 15, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.BID, 10, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    assertEquals(OptionalLong.of(15L), orderBook.bestBidPrice());
    orderBook.cancel(2L);
    assertEquals(OptionalLong.of(10L), orderBook.bestBidPrice());
  }

  @Test
  void returnsEmptyBestBidAfterLastBidIsCancelled() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 1, 5L);
    orderBook.add(order);
    assertEquals(OptionalLong.of(1L), orderBook.bestBidPrice());
    orderBook.cancel(1L);
    assertEquals(OptionalLong.empty(), orderBook.bestBidPrice());
  }

  @Test
  void returnsEmptyBestAskWhenNoAsksExist() {
    OrderBook orderBook = new OrderBook();
    assertEquals(OptionalLong.empty(), orderBook.bestAskPrice());
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 1L, 5L);
    orderBook.add(order);
    assertEquals(OptionalLong.empty(), orderBook.bestAskPrice());
  }

  @Test
  void returnsLowestAskPrice() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.ASK, 15L, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.ASK, 5L, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.ASK, 10L, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    assertEquals(OptionalLong.of(5L), orderBook.bestAskPrice());
  }

  @Test
  void ignoresBidsWhenFindingBestAsk() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.ASK, 15L, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.ASK, 5L, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.ASK, 10L, 5L);
    PlaceOrder order4 = new PlaceOrder(4L, Side.BID, 1L, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    orderBook.add(order4);
    assertEquals(OptionalLong.of(5L), orderBook.bestAskPrice());
  }

  @Test
  void returnsNextBestAskAfterCancellation() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order1 = new PlaceOrder(1L, Side.ASK, 15L, 5L);
    PlaceOrder order2 = new PlaceOrder(2L, Side.ASK, 5L, 5L);
    PlaceOrder order3 = new PlaceOrder(3L, Side.ASK, 10L, 5L);
    orderBook.add(order1);
    orderBook.add(order2);
    orderBook.add(order3);
    assertEquals(OptionalLong.of(5L), orderBook.bestAskPrice());
    orderBook.cancel(2L);
    assertEquals(OptionalLong.of(10L), orderBook.bestAskPrice());
  }

  @Test
  void returnsEmptyBestAskAfterLastAskIsCancelled() {
    OrderBook orderBook = new OrderBook();
    PlaceOrder order = new PlaceOrder(1L, Side.ASK, 1L, 5L);
    orderBook.add(order);
    assertEquals(OptionalLong.of(1L), orderBook.bestAskPrice());
    orderBook.cancel(1L);
    assertEquals(OptionalLong.empty(), orderBook.bestAskPrice());
  }

  @Test
  void maintainsBookEntryOrderBestBid() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 90L;
    long placeOrderID2 = 3L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 1, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.BID, 1, 10L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(order1, orderBook.bestBid().get());
  }

  @Test
  void maintainsBookEntryOrderBestAsk() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 90L;
    long placeOrderID2 = 3L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.ASK, 1, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.ASK, 1, 10L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(order1, orderBook.bestAsk().get());
  }

  @Test
  void maintainsBookEntryMultipleOrderBestBid() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 90L;
    long placeOrderID2 = 3L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 1, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.BID, 1, 10L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(order1, orderBook.bestBid().get());
    long placeOrderID3 = 10L;
    PlaceOrder order3 = new PlaceOrder(placeOrderID3, Side.BID, 2, 5L);
    orderBook.add(order3);
    assertEquals(order3, orderBook.bestBid().get());
  }

  @Test
  void maintainsBookEntryMultipleOrderBestAsk() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 90L;
    long placeOrderID2 = 3L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.ASK, 2, 5L);
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.ASK, 2, 10L);
    orderBook.add(order1);
    orderBook.add(order2);
    assertEquals(order1, orderBook.bestAsk().get());
    long placeOrderID3 = 10L;
    PlaceOrder order3 = new PlaceOrder(placeOrderID3, Side.ASK, 1, 5L);
    orderBook.add(order3);
    assertEquals(order3, orderBook.bestAsk().get());
  }

  @Test
  void maintainsRemainingQuantityNewOrder() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 90L;
    long placeOrderQuantityLots = 5L;
    PlaceOrder order = new PlaceOrder(placeOrderID1, Side.ASK, 2, placeOrderQuantityLots);
    orderBook.add(order);
    long placeOrderID2 = 3L;
    assertEquals(OptionalLong.of(placeOrderQuantityLots), orderBook.remainingLots(placeOrderID1));
    assertEquals(OptionalLong.empty(), orderBook.remainingLots(placeOrderID2));
  }

  @Test
  void maintainsOriginalPlaceOrderAfterPartialFill() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    long placeOrderQuantityLots = 5L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.ASK, 2, placeOrderQuantityLots);
    orderBook.add(order);
    orderBook.fill(placeOrderID, 2L);
    assertEquals(1, orderBook.size());
    assertEquals(OptionalLong.of(3L), orderBook.remainingLots(placeOrderID));
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
  }

  @Test
  void removesFilledRestingOrder() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    long placeOrderQuantityLots = 5L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 2, placeOrderQuantityLots);
    orderBook.add(order);
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.bestBid());
    orderBook.fill(placeOrderID, placeOrderQuantityLots);
    assertEquals(0, orderBook.size());
    assertEquals(Optional.empty(), orderBook.bestBid());
  }

  @Test
  void rejectsInvalidFillQuantities() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID, Side.BID, 2, 5L);
    orderBook.add(order);

    assertThrows(IllegalArgumentException.class, () -> orderBook.fill(placeOrderID, -2L));
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(placeOrderID));

    assertThrows(IllegalArgumentException.class, () -> orderBook.fill(placeOrderID, 0L));
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(placeOrderID));

    assertThrows(IllegalArgumentException.class, () -> orderBook.fill(placeOrderID, 10L));
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(placeOrderID));
  }

  @Test
  void rejectsInvalidFillId() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 1L;
    PlaceOrder order = new PlaceOrder(placeOrderID1, Side.BID, 2, 5L);
    orderBook.add(order);
    long placeOrderID2 = 2L;

    assertThrows(IllegalArgumentException.class, () -> orderBook.fill(placeOrderID2, 2L));
    assertEquals(1, orderBook.size());
    assertEquals(Optional.of(order), orderBook.find(placeOrderID1));
    assertEquals(OptionalLong.of(5L), orderBook.remainingLots(placeOrderID1));
  }

  @Test
  void preservesOrderAfterFills() {
    OrderBook orderBook = new OrderBook();
    long placeOrderID1 = 1L;
    PlaceOrder order1 = new PlaceOrder(placeOrderID1, Side.BID, 2, 5L);
    long placeOrderID2 = 2L;
    PlaceOrder order2 = new PlaceOrder(placeOrderID2, Side.BID, 2, 5L);
    orderBook.add(order1);
    orderBook.add(order2);

    assertEquals(Optional.of(order1), orderBook.bestBid());
    orderBook.fill(placeOrderID1, 3L);
    assertEquals(Optional.of(order1), orderBook.bestBid());
    orderBook.fill(placeOrderID1, 2L);
    assertEquals(Optional.of(order2), orderBook.bestBid());
  }
}
