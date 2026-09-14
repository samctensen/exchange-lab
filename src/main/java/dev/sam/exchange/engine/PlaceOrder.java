package dev.sam.exchange.engine;

public record PlaceOrder(long orderId, Side side, long priceTicks, long quantityLots) {
  public PlaceOrder {
    if (quantityLots <= 0) {
      throw new IllegalArgumentException("quantityLots must be positive");
    }
    if (priceTicks <= 0) {
      throw new IllegalArgumentException("priceTicks must be positive");
    }
    if (side == null) {
      throw new IllegalArgumentException("side must not be null");
    }
  }
}
