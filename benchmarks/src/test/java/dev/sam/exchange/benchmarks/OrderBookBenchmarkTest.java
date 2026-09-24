package dev.sam.exchange.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Trade;

class OrderBookBenchmarkTest {
  @ParameterizedTest
  @ValueSource(ints = {1000, 10000, 100000})
  void passiveCycleCancelsItsOrderWithoutChangingRestingOrders(int depth) {
    OrderBookBenchmark benchmark = fixture(depth);
    List<OrderSnapshot> before = canonicalSnapshot(benchmark);
    for (int cycle = 0; cycle < 8; cycle++) {
      PlaceResult result = benchmark.passivePlaceAndCancel();
      assertNotNull(result);
      assertEquals(new PlaceResult(depth + 1L, List.of(), 1L), result);
      assertRestored(benchmark, before);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1000, 10000, 100000})
  void singleFillCycleContinuesTradingAfterEachRefill(int depth) {
    OrderBookBenchmark benchmark = fixture(depth);
    List<OrderSnapshot> before = canonicalSnapshot(benchmark);
    for (int cycle = 0; cycle < 8; cycle++) {
      PlaceResult result = benchmark.singleFillAndRefill();
      assertNotNull(result);
      assertEquals(new PlaceResult(depth + 1L, List.of(new Trade(depth + 1L, depth - 3L, 100L, 1L)), 0L), result);
      assertRestored(benchmark, before);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1000, 10000, 100000})
  void sweepCycleTradesAtFourRestingPricesAndRestoresEveryOrder(int depth) {
    OrderBookBenchmark benchmark = fixture(depth);
    List<OrderSnapshot> before = canonicalSnapshot(benchmark);
    for (int cycle = 0; cycle < 8; cycle++) {
      PlaceResult result = benchmark.fourFillsAndRefill();
      assertNotNull(result);
      assertEquals(new PlaceResult(depth + 1L,
          List.of(new Trade(depth + 1L, depth - 3L, 100L, 1L), new Trade(depth + 1L, depth - 2L, 101L, 1L),
              new Trade(depth + 1L, depth - 1L, 102L, 1L), new Trade(depth + 1L, depth, 103L, 1L)),
          0L), result);
      assertRestored(benchmark, before);
    }
  }

  private static OrderBookBenchmark fixture(int depth) {
    OrderBookBenchmark benchmark = new OrderBookBenchmark();
    benchmark.depth = depth;
    benchmark.setup();
    assertEquals(depth, benchmark.book.size());
    assertEquals(90L, benchmark.book.bestBidPrice().orElseThrow());
    assertEquals(100L, benchmark.book.bestAskPrice().orElseThrow());
    return benchmark;
  }

  private static void assertRestored(OrderBookBenchmark benchmark, List<OrderSnapshot> before) {
    assertEquals(benchmark.depth, benchmark.book.size());
    assertFalse(benchmark.book.find(benchmark.depth + 1L).isPresent());
    assertEquals(before, canonicalSnapshot(benchmark));
  }

  private static List<OrderSnapshot> canonicalSnapshot(OrderBookBenchmark benchmark) {
    // Refilling distinct price levels changes global insertion order, but not price-time priority.
    return benchmark.book.snapshot().stream().sorted(Comparator.comparingLong(snapshot -> snapshot.order().orderId()))
        .toList();
  }
}
