package dev.sam.exchange.engine;

import java.util.Objects;

/** Immutable view of an order currently resting on the book. */
public record OrderSnapshot(PlaceOrder order, long remainingLots) {
  public OrderSnapshot {
    Objects.requireNonNull(order, "order must not be null");
    if (remainingLots <= 0 || remainingLots > order.quantityLots()) {
      throw new IllegalArgumentException("Snapshot remaining lots must be positive and not exceed order quantity");
    }
  }
}
