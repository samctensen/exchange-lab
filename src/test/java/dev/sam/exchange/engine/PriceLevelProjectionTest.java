package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class PriceLevelProjectionTest {
  @ParameterizedTest
  @EnumSource(Side.class)
  void projectsAnEmptyLevel(Side side) {
    assertEquals(new PriceLevelSnapshot(100L, 0, BigInteger.ZERO), new PriceLevel(side, 100L).snapshot());
  }

  @Test
  void sumsRemainingLotsBeyondLongRangeWithoutMutatingOrders() {
    PriceLevel level = new PriceLevel(Side.BID, 100L);
    level.add(new RestingOrder(new PlaceOrder(20L, Side.BID, 100L, Long.MAX_VALUE)));
    level.add(new RestingOrder(new PlaceOrder(10L, Side.BID, 100L, Long.MAX_VALUE)));
    level.first().orElseThrow().fill(2L);

    PriceLevelSnapshot snapshot = level.snapshot();

    assertEquals(new PriceLevelSnapshot(100L, 2, new BigInteger("18446744073709551612")), snapshot);
    assertEquals(20L, level.first().orElseThrow().order().orderId());
    assertEquals(Long.MAX_VALUE - 2L, level.first().orElseThrow().remainingLots());
    level.first().orElseThrow().fill(3L);
    level.remove(10L);
    assertEquals(new PriceLevelSnapshot(100L, 1, new BigInteger("9223372036854775802")), level.snapshot());
    assertEquals(new BigInteger("18446744073709551612"), snapshot.quantityLots());
  }

  @Test
  void fullyFilledOrdersNoLongerContributeToProjection() {
    PriceLevel level = new PriceLevel(Side.ASK, 101L);
    level.add(new RestingOrder(new PlaceOrder(1L, Side.ASK, 101L, 5L)));
    level.first().orElseThrow().fill(5L);

    assertEquals(new PriceLevelSnapshot(101L, 0, BigInteger.ZERO), level.snapshot());
  }

  @ParameterizedTest
  @CsvSource({"0, 0, 0", "-1, 0, 0", "100, -1, 0", "100, 1, -1", "100, 0, 1", "100, 1, 0"})
  void rejectsInvalidProjection(long priceTicks, int orderCount, String quantityLots) {
    assertThrows(IllegalArgumentException.class,
        () -> new PriceLevelSnapshot(priceTicks, orderCount, new BigInteger(quantityLots)));
  }

  @Test
  void rejectsNullQuantity() {
    assertThrows(NullPointerException.class, () -> new PriceLevelSnapshot(100L, 0, null));
  }
}
