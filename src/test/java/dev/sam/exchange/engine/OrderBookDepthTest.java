package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OrderBookDepthTest {
  @Test
  void limitsEachSideInBestPriceOrderUsingRemainingLots() {
    OrderBook book = populatedBook();

    assertEquals(
        new BookDepth(List.of(level(100L, 2, 13L), level(99L, 1, 8L)), List.of(level(102L, 2, 8L), level(103L, 1, 7L))),
        book.depth(2));
    assertEquals(new BookDepth(List.of(level(100L, 2, 13L), level(99L, 1, 8L), level(98L, 1, 4L)),
        List.of(level(102L, 2, 8L), level(103L, 1, 7L), level(104L, 1, 9L))), book.depth(Integer.MAX_VALUE));
  }

  @Test
  void acceptsZeroDepthAndEmptyOrOneSidedBooks() {
    OrderBook empty = new OrderBook();
    assertEquals(new BookDepth(List.of(), List.of()), empty.depth(5));
    assertEquals(new BookDepth(List.of(), List.of()), populatedBook().depth(0));
    empty.add(new PlaceOrder(1L, Side.ASK, 101L, 7L));
    assertEquals(new BookDepth(List.of(), List.of(level(101L, 1, 7L))), empty.depth(1));
  }

  @Test
  void rejectsNegativeDepthEvenWhenBookIsEmpty() {
    assertThrows(IllegalArgumentException.class, () -> new OrderBook().depth(-1));
  }

  @Test
  void projectionsRemainUnchangedAfterFillsCancellationsAndNewOrders() {
    OrderBook book = populatedBook();
    BookDepth before = book.depth(1);
    book.fill(20L, 7L);
    book.cancel(40L);
    book.cancel(60L);
    book.add(new PlaceOrder(90L, Side.ASK, 101L, 2L));

    assertEquals(new BookDepth(List.of(level(99L, 1, 8L)), List.of(level(101L, 1, 2L))), book.depth(1));
    assertEquals(new BookDepth(List.of(level(100L, 2, 13L)), List.of(level(102L, 2, 8L))), before);
  }

  @Test
  void readsDoNotChangeRemainingLotsOrFifoPriority() {
    OrderBook book = populatedBook();
    List<OrderSnapshot> before = book.snapshot();
    for (int depth : new int[]{0, 1, 2, Integer.MAX_VALUE}) {
      book.depth(depth);
    }

    assertEquals(before, book.snapshot());
    assertEquals(Optional.of(new PlaceOrder(20L, Side.BID, 100L, 10L)), book.bestBid());
    assertEquals(Optional.of(new PlaceOrder(60L, Side.ASK, 102L, 5L)), book.bestAsk());
  }

  @Test
  void depthCopiesInputListsAndRejectsMutation() {
    ArrayList<PriceLevelSnapshot> bids = new ArrayList<>(List.of(level(100L, 1, 2L)));
    ArrayList<PriceLevelSnapshot> asks = new ArrayList<>(List.of(level(101L, 1, 3L)));
    BookDepth depth = new BookDepth(bids, asks);
    bids.clear();
    asks.clear();

    assertEquals(List.of(level(100L, 1, 2L)), depth.bids());
    assertEquals(List.of(level(101L, 1, 3L)), depth.asks());
    assertThrows(UnsupportedOperationException.class, () -> depth.bids().clear());
    assertThrows(UnsupportedOperationException.class, () -> depth.asks().clear());
  }

  @Test
  void depthRejectsNullListsAndElements() {
    assertThrows(NullPointerException.class, () -> new BookDepth(null, List.of()));
    assertThrows(NullPointerException.class, () -> new BookDepth(List.of(), null));
    assertThrows(NullPointerException.class, () -> new BookDepth(Arrays.asList((PriceLevelSnapshot) null), List.of()));
    assertThrows(NullPointerException.class, () -> new BookDepth(List.of(), Arrays.asList((PriceLevelSnapshot) null)));
  }

  private static PriceLevelSnapshot level(long price, int orders, long lots) {
    return new PriceLevelSnapshot(price, orders, BigInteger.valueOf(lots));
  }

  private static OrderBook populatedBook() {
    OrderBook book = new OrderBook();
    book.add(new PlaceOrder(10L, Side.BID, 98L, 4L));
    book.add(new PlaceOrder(20L, Side.BID, 100L, 10L));
    book.add(new PlaceOrder(30L, Side.BID, 99L, 8L));
    book.add(new PlaceOrder(40L, Side.BID, 100L, 6L));
    book.add(new PlaceOrder(50L, Side.ASK, 104L, 9L));
    book.add(new PlaceOrder(60L, Side.ASK, 102L, 5L));
    book.add(new PlaceOrder(70L, Side.ASK, 103L, 7L));
    book.add(new PlaceOrder(80L, Side.ASK, 102L, 3L));
    book.fill(20L, 3L);
    return book;
  }
}
