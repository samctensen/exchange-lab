package dev.sam.exchange.engine;

import java.math.BigInteger;
import java.util.Objects;

/** Immutable counts and exact remaining quantities across every level of both sides. */
public record BookSummary(int bidOrders, int askOrders, int bidLevels, int askLevels, BigInteger bidLots,
    BigInteger askLots) {
  public BookSummary {
    Objects.requireNonNull(bidLots, "bidLots must not be null");
    Objects.requireNonNull(askLots, "askLots must not be null");
    validateSide(bidOrders, bidLevels, bidLots);
    validateSide(askOrders, askLevels, askLots);
  }

  private static void validateSide(int orders, int levels, BigInteger lots) {
    if (orders < 0 || levels < 0 || lots.signum() < 0) {
      throw new IllegalArgumentException("order counts, level counts and lots must not be negative");
    }
    if (levels > orders || (orders == 0) != (levels == 0) || (orders == 0) != (lots.signum() == 0)) {
      throw new IllegalArgumentException("order counts, level counts and lots must describe the same live orders");
    }
  }
}
