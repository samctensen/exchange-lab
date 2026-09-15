package dev.sam.exchange.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MatchingEngine {

  private OrderBook orderBook;

  public MatchingEngine(OrderBook orderBook) {
    this.orderBook = orderBook;
  }

  public CommandResult process(EngineCommand command) {
    return switch (command) {
      case CancelOrder cancel -> {
        Optional<PlaceOrder> cancelled = this.orderBook.cancel(cancel.orderId());
        CancelResult result = new CancelResult(cancel.orderId(), cancelled.isPresent());
        yield result;
      }
      case PlaceOrder order -> {
        List<Trade> trades = this.submit(order);
        long remainingLots = this.orderBook.remainingLots(order.orderId()).orElse(0L);
        PlaceResult result = new PlaceResult(order.orderId(), trades, remainingLots);
        yield result;
      }
    };
  }

  public List<Trade> submit(PlaceOrder order) {
    this.orderBook.add(order);
    return this.match(order.orderId());
  }

  public List<Trade> match(long incomingOrderId) {
    List<Trade> matches = new ArrayList<Trade>();
    Optional<Trade> match = matchOnce(incomingOrderId);

    while (match.isPresent()) {
      matches.add(match.get());

      if (this.orderBook.find(incomingOrderId).isPresent()) {
        match = matchOnce(incomingOrderId);
      } else {
        match = Optional.empty();
      }
    }

    return matches;
  }

  public Optional<Trade> matchOnce(long incomingOrderId) {
    PlaceOrder incomingOrder = findIncomingOrderOrThrow(incomingOrderId);

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

  private PlaceOrder findIncomingOrderOrThrow(long incomingOrderId) {
    Optional<PlaceOrder> incomingOrderOptional = this.orderBook.find(incomingOrderId);
    if (incomingOrderOptional.isEmpty()) {
      throw new IllegalArgumentException("Incoming order ID does not exist: " + incomingOrderId);
    }

    return incomingOrderOptional.get();
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
