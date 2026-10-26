package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OrderBookProjectionSequenceTest {
  @ParameterizedTest
  @ValueSource(longs = {17L, 20261026L, 9223372036854775807L})
  void mixedOperationsAgreeWithIndependentLedgerAndPreserveFifo(long seed) {
    OrderBook book = new OrderBook();
    List<LiveOrder> ledger = new ArrayList<>();
    Random random = new Random(seed);
    for (PlaceOrder order : List.of(new PlaceOrder(20L, Side.BID, 100L, 9L), new PlaceOrder(10L, Side.BID, 100L, 5L),
        new PlaceOrder(30L, Side.BID, 99L, Long.MAX_VALUE), new PlaceOrder(21L, Side.ASK, 101L, 7L),
        new PlaceOrder(11L, Side.ASK, 101L, 3L), new PlaceOrder(31L, Side.ASK, 102L, Long.MAX_VALUE))) {
      add(book, ledger, order, "initial orders");
    }
    verify(book, ledger, "seed=" + seed + ", initial orders");

    for (int step = 0; step < 500; step++) {
      String context = "seed=" + seed + ", step=" + step;
      long id = 1L + random.nextInt(40);
      LiveOrder existing = find(ledger, id).orElse(null);
      switch (random.nextInt(5)) {
        case 0, 1 -> {
          Side side = random.nextBoolean() ? Side.BID : Side.ASK;
          long lots = random.nextInt(8) == 0 ? Long.MAX_VALUE : 1L + random.nextInt(20);
          add(book, ledger, new PlaceOrder(id, side, 97L + random.nextInt(7), lots), context);
        }
        case 2 -> {
          assertEquals(Optional.ofNullable(existing).map(LiveOrder::original), book.cancel(id), context);
          ledger.remove(existing);
        }
        case 3 -> {
          if (existing == null) {
            assertThrows(IllegalArgumentException.class, () -> book.fill(id, 1L), context);
          } else {
            long remaining = existing.remaining().longValueExact();
            long lots = random.nextBoolean() ? remaining : 1L + random.nextLong(remaining);
            fill(book, ledger, existing, lots);
          }
        }
        case 4 -> {
          long invalidLots = existing != null && existing.remaining().compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0
              ? existing.remaining().longValueExact() + 1L
              : 0L;
          assertThrows(IllegalArgumentException.class, () -> book.fill(id, invalidLots), context);
        }
        default -> throw new AssertionError("Unexpected operation");
      }
      verify(book, ledger, context);
    }

    while (!ledger.isEmpty()) {
      LiveOrder order = ledger.get(random.nextInt(ledger.size()));
      fill(book, ledger, order, order.remaining().longValueExact());
      verify(book, ledger, "seed=" + seed + ", draining book");
    }
  }

  private static void add(OrderBook book, List<LiveOrder> ledger, PlaceOrder order, String context) {
    if (find(ledger, order.orderId()).isPresent()) {
      assertThrows(IllegalArgumentException.class, () -> book.add(order), context);
    } else {
      book.add(order);
      ledger.add(new LiveOrder(order, BigInteger.valueOf(order.quantityLots())));
    }
  }

  private static void fill(OrderBook book, List<LiveOrder> ledger, LiveOrder order, long lots) {
    book.fill(order.original().orderId(), lots);
    BigInteger remaining = order.remaining().subtract(BigInteger.valueOf(lots));
    if (remaining.signum() == 0) {
      ledger.remove(order);
    } else {
      ledger.set(ledger.indexOf(order), new LiveOrder(order.original(), remaining));
    }
  }

  private static Optional<LiveOrder> find(List<LiveOrder> ledger, long id) {
    return ledger.stream().filter(order -> order.original().orderId() == id).findFirst();
  }

  private static void verify(OrderBook book, List<LiveOrder> ledger, String context) {
    List<PriceLevelSnapshot> bids = expectedLevels(ledger, Side.BID);
    List<PriceLevelSnapshot> asks = expectedLevels(ledger, Side.ASK);
    BookSummary summary = new BookSummary(orderCount(bids), orderCount(asks), bids.size(), asks.size(), totalLots(bids),
        totalLots(asks));

    assertEquals(summary, book.summary(), context);
    for (int limit : new int[]{0, 1, 3, Integer.MAX_VALUE}) {
      BookDepth expected = new BookDepth(bids.subList(0, Math.min(limit, bids.size())),
          asks.subList(0, Math.min(limit, asks.size())));
      assertEquals(expected, book.depth(limit), context + ", depth=" + limit);
    }
    assertEquals(summary, book.summary(), context + ", after depth reads");
    assertEquals(expectedBest(ledger, bids, Side.BID), book.bestBid(), context);
    assertEquals(expectedBest(ledger, asks, Side.ASK), book.bestAsk(), context);
    assertEquals(ledger.size(), book.size(), context);
    assertEquals(
        ledger.stream().map(order -> new OrderSnapshot(order.original(), order.remaining().longValueExact())).toList(),
        book.snapshot(), context);
    for (long id = 1; id <= 40; id++) {
      Optional<LiveOrder> order = find(ledger, id);
      assertEquals(order.map(LiveOrder::original), book.find(id), context + ", id=" + id);
      OptionalLong expected = order.isPresent()
          ? OptionalLong.of(order.orElseThrow().remaining().longValueExact())
          : OptionalLong.empty();
      assertEquals(expected, book.remainingLots(id), context + ", id=" + id);
    }
  }

  // The oracle uses a flat arrival ledger, BigInteger subtraction and a fixed price scan,
  // independently of the book's maps, mutable RestingOrder objects and price-level queues.
  private static List<PriceLevelSnapshot> expectedLevels(List<LiveOrder> ledger, Side side) {
    List<PriceLevelSnapshot> levels = new ArrayList<>();
    for (int offset = 0; offset < 7; offset++) {
      long price = side == Side.BID ? 103L - offset : 97L + offset;
      int orders = 0;
      BigInteger lots = BigInteger.ZERO;
      for (LiveOrder order : ledger) {
        if (order.original().side() == side && order.original().priceTicks() == price) {
          orders++;
          lots = lots.add(order.remaining());
        }
      }
      if (orders > 0) {
        levels.add(new PriceLevelSnapshot(price, orders, lots));
      }
    }
    return levels;
  }

  private static Optional<PlaceOrder> expectedBest(List<LiveOrder> ledger, List<PriceLevelSnapshot> levels, Side side) {
    if (levels.isEmpty()) {
      return Optional.empty();
    }
    long bestPrice = levels.getFirst().priceTicks();
    return ledger.stream().map(LiveOrder::original)
        .filter(order -> order.side() == side && order.priceTicks() == bestPrice).findFirst();
  }

  private static int orderCount(List<PriceLevelSnapshot> levels) {
    return levels.stream().mapToInt(PriceLevelSnapshot::orderCount).sum();
  }

  private static BigInteger totalLots(List<PriceLevelSnapshot> levels) {
    return levels.stream().map(PriceLevelSnapshot::quantityLots).reduce(BigInteger.ZERO, BigInteger::add);
  }

  private record LiveOrder(PlaceOrder original, BigInteger remaining) {
  }
}
