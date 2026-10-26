package dev.sam.exchange.engine;

import java.math.BigInteger;
import java.util.Objects;

/** Immutable quantities of live orders at one price, expressed without aggregate overflow. */
public record PriceLevelSnapshot(long priceTicks, int orderCount, BigInteger quantityLots) {
  public PriceLevelSnapshot {
    Objects.requireNonNull(quantityLots, "quantityLots must not be null");
    if (priceTicks <= 0L) {
      throw new IllegalArgumentException("priceTicks must be greater than zero");
    }
    if (orderCount < 0 || quantityLots.signum() < 0) {
      throw new IllegalArgumentException("orderCount and quantityLots must not be negative");
    }
    if ((orderCount == 0) != (quantityLots.signum() == 0)) {
      throw new IllegalArgumentException("orderCount and quantityLots must be zero together");
    }
  }
}
