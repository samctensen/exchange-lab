package dev.sam.exchange.engine;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class PriceLevel {
  private final Side side;
  private final long priceTicks;
  private final LinkedHashMap<Long, RestingOrder> orders = new LinkedHashMap<>();

  public PriceLevel(Side side, long priceTicks) {
    Objects.requireNonNull(side, "side must not be null");
    if (priceTicks <= 0) {
      throw new IllegalArgumentException("priceTicks must be greater than zero");
    }
    this.side = side;
    this.priceTicks = priceTicks;
  }

  void add(RestingOrder order) {
    if (order.order().side() != this.side) {
      throw new IllegalArgumentException("order side must match price level side");
    }
    if (order.order().priceTicks() != this.priceTicks) {
      throw new IllegalArgumentException("order price must match price level price");
    }
    if (this.orders.containsKey(order.order().orderId())) {
      throw new IllegalArgumentException("order id must be unique");
    }
    this.orders.put(order.order().orderId(), order);
  }

  public Optional<RestingOrder> first() {
    return Optional.ofNullable(this.orders.firstEntry()).map(Map.Entry::getValue);
  }

  public Optional<RestingOrder> remove(long orderId) {
    return Optional.ofNullable(this.orders.remove(orderId));
  }

  /** Reads current remaining lots on the owning thread, excluding fully filled orders. */
  public PriceLevelSnapshot snapshot() {
    int orderCount = 0;
    BigInteger quantityLots = BigInteger.ZERO;
    for (RestingOrder order : orders.values()) {
      long remainingLots = order.remainingLots();
      if (remainingLots > 0L) {
        orderCount++;
        quantityLots = quantityLots.add(BigInteger.valueOf(remainingLots));
      }
    }
    return new PriceLevelSnapshot(priceTicks, orderCount, quantityLots);
  }

  public boolean isEmpty() {
    return this.orders.isEmpty();
  }
}
