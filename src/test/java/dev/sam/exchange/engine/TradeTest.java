package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TradeTest {
  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void rejectsNonPositivePrice(long price) {
    assertThrows(IllegalArgumentException.class, () -> new Trade(1, 2, price, 1));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void rejectsNonPositiveQuantity(long quantity) {
    assertThrows(IllegalArgumentException.class, () -> new Trade(1, 2, 1, quantity));
  }

  @Test
  void preservesFullWidthIdsAndPositiveAmounts() {
    Trade trade = new Trade(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    assertEquals(Long.MIN_VALUE, trade.incomingOrderId());
    assertEquals(Long.MAX_VALUE, trade.restingOrderId());
    assertEquals(Long.MAX_VALUE, trade.priceTicks());
    assertEquals(Long.MAX_VALUE, trade.quantityLots());
  }
}
