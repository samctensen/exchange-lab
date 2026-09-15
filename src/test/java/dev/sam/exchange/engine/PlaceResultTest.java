package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class PlaceResultTest {

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
