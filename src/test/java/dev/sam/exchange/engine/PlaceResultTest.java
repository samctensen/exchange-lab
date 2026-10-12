package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PlaceResultTest {

  @ParameterizedTest
  @ValueSource(longs = {-1, Long.MIN_VALUE})
  void rejectsNegativeRemainingLots(long remainingLots) {
    assertThrows(IllegalArgumentException.class, () -> new PlaceResult(1, List.of(), remainingLots));
  }

  @Test
  void allowsFullyFilledAndMaximumSizeResults() {
    assertEquals(0, new PlaceResult(Long.MIN_VALUE, List.of(), 0).remainingLots());
    assertEquals(Long.MAX_VALUE, new PlaceResult(Long.MAX_VALUE, List.of(), Long.MAX_VALUE).remainingLots());
  }

  @Test
  void retainsTradesWhenSourceListChanges() {
    Trade trade = new Trade(2L, 1L, 100L, 2L);
    List<Trade> trades = new ArrayList<>(List.of(trade));
    PlaceResult result = new PlaceResult(2L, trades, 0L);

    trades.clear();

    assertEquals(List.of(trade), result.trades());
  }

  @Test
  void doesNotAllowTradesToBeModifiedThroughResult() {
    Trade trade = new Trade(2L, 1L, 100L, 2L);
    List<Trade> trades = new ArrayList<>(List.of(trade));
    PlaceResult result = new PlaceResult(2L, trades, 0L);

    assertThrows(UnsupportedOperationException.class, () -> result.trades().clear());
    assertEquals(List.of(trade), result.trades());
  }
}
