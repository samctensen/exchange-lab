package dev.sam.exchange.engine;

public class RestingOrder {
  private final PlaceOrder order;
  private long remainingLots;

  public RestingOrder(PlaceOrder order) {
    this.order = order;
    this.remainingLots = order.quantityLots();
  }

  public PlaceOrder order() {
    return this.order;
  }

  public long remainingLots() {
    return this.remainingLots;
  }

  public void fill(long quantityLots) {
    if (quantityLots <= 0) {
      throw new IllegalArgumentException("quantityLots must be greater than zero");
    }

    if (quantityLots > this.remainingLots()) {
      throw new IllegalArgumentException("quantityLots must not exceed remainingLots");
    }

    this.remainingLots -= quantityLots;
  }
}
