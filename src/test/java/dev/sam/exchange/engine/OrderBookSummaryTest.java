package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class OrderBookSummaryTest {
  @Test
  void summarizesAnEmptyBook() {
    assertEquals(new BookSummary(0, 0, 0, 0, BigInteger.ZERO, BigInteger.ZERO), new OrderBook().summary());
  }

  @Test
  void aggregatesEveryLevelAndCurrentQuantityBeyondLongRange() {
    OrderBook book = largeBook();
    book.fill(1L, 2L);
    BookSummary expected = new BookSummary(3, 2, 2, 2, new BigInteger("18446744073709551616"),
        new BigInteger("18446744073709551614"));

    assertEquals(expected, book.summary());
    book.depth(1);
    assertEquals(expected, book.summary());
    book.depth(0);
    assertEquals(expected, book.summary());
  }

  @Test
  void fillsAndCancellationsUpdateCountsWhileEarlierSummaryRemainsUnchanged() {
    OrderBook book = largeBook();
    BookSummary before = book.summary();
    book.fill(1L, Long.MAX_VALUE);
    book.cancel(3L);
    book.cancel(4L);
    book.fill(5L, Long.MAX_VALUE);

    assertEquals(new BookSummary(1, 0, 1, 0, BigInteger.valueOf(Long.MAX_VALUE), BigInteger.ZERO), book.summary());
    assertEquals(
        new BookSummary(3, 2, 2, 2, new BigInteger("18446744073709551618"), new BigInteger("18446744073709551614")),
        before);
    book.cancel(2L);
    assertEquals(new BookSummary(0, 0, 0, 0, BigInteger.ZERO, BigInteger.ZERO), book.summary());
  }

  @ParameterizedTest
  @CsvSource({"-1, 0, 0", "0, -1, 0", "1, 2, 1", "0, 1, 0", "1, 0, 1", "0, 0, 1", "1, 1, 0", "1, 1, -1"})
  void rejectsInconsistentSideTotals(int orders, int levels, String lots) {
    BigInteger quantity = new BigInteger(lots);
    assertThrows(IllegalArgumentException.class,
        () -> new BookSummary(orders, 0, levels, 0, quantity, BigInteger.ZERO));
    assertThrows(IllegalArgumentException.class,
        () -> new BookSummary(0, orders, 0, levels, BigInteger.ZERO, quantity));
  }

  @Test
  void rejectsNullQuantities() {
    assertThrows(NullPointerException.class, () -> new BookSummary(0, 0, 0, 0, null, BigInteger.ZERO));
    assertThrows(NullPointerException.class, () -> new BookSummary(0, 0, 0, 0, BigInteger.ZERO, null));
  }

  private static OrderBook largeBook() {
    OrderBook book = new OrderBook();
    book.add(new PlaceOrder(1L, Side.BID, 100L, Long.MAX_VALUE));
    book.add(new PlaceOrder(2L, Side.BID, 100L, Long.MAX_VALUE));
    book.add(new PlaceOrder(3L, Side.BID, 99L, 4L));
    book.add(new PlaceOrder(4L, Side.ASK, 101L, Long.MAX_VALUE));
    book.add(new PlaceOrder(5L, Side.ASK, 102L, Long.MAX_VALUE));
    return book;
  }
}
