package dev.sam.exchange.engine;

import java.util.Optional;

public class MatchingEngine {

  private OrderBook orderBook;

  public MatchingEngine(OrderBook orderBook) {
    this.orderBook = orderBook;
  }

  public Optional<Trade> matchOnce(long incomingOrderId) {
    Optional<PlaceOrder> incomingOrderOptional = this.orderBook.find(incomingOrderId);
    if (incomingOrderOptional.isEmpty()) {
      throw new IllegalArgumentException("Incoming order ID does not exist: " + incomingOrderId);
    }

    PlaceOrder incomingOrder = incomingOrderOptional.get();

    if (incomingOrder.side() == Side.BID) {
      Optional<PlaceOrder> restingBestAsk = this.orderBook.bestAsk();
      if (restingBestAsk.isEmpty() || restingBestAsk.get().priceTicks() > incomingOrder.priceTicks()) {
        return Optional.empty();
      }
      return Optional
          .of(createTrade(incomingOrder.orderId(), restingBestAsk.get().orderId(), restingBestAsk.get().priceTicks()));
    } else {
      Optional<PlaceOrder> restingBestBid = this.orderBook.bestBid();
      if (restingBestBid.isEmpty() || restingBestBid.get().priceTicks() < incomingOrder.priceTicks()) {
        return Optional.empty();
      }
      return Optional
          .of(createTrade(incomingOrder.orderId(), restingBestBid.get().orderId(), restingBestBid.get().priceTicks()));
    }
  }

  private Trade createTrade(long incomingOrderId, long restingOrderId, long restingPriceTicks) {
    long incomingRemaining = orderBook.remainingLots(incomingOrderId).orElseThrow();
    long restingRemaining = orderBook.remainingLots(restingOrderId).orElseThrow();
    long quantityLots = Math.min(incomingRemaining, restingRemaining);

    this.orderBook.fill(incomingOrderId, quantityLots);
    this.orderBook.fill(restingOrderId, quantityLots);

    Trade trade = new Trade(incomingOrderId, restingOrderId, restingPriceTicks, quantityLots);
    return trade;
  }
}
