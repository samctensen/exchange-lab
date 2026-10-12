package dev.sam.exchange.engine;

public record Trade(long incomingOrderId, long restingOrderId, long priceTicks, long quantityLots) {
  public Trade {
    if (priceTicks <= 0 || quantityLots <= 0) {
      throw new IllegalArgumentException("Trade price and quantity must be positive");
    }
  }
}
