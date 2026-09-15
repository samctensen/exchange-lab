package dev.sam.exchange.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

public class OrderBook {

  private final Map<Long, RestingOrder> ordersById = new LinkedHashMap<>();

  public void add(PlaceOrder order) {
    if (ordersById.containsKey(order.orderId())) {
      throw new IllegalArgumentException("Order ID already exists: " + order.orderId());
    }
    RestingOrder restingOrder = new RestingOrder(order);
    ordersById.put(order.orderId(), restingOrder);
  }

  public Optional<PlaceOrder> find(long orderId) {
    return Optional.ofNullable(ordersById.get(orderId)).map(RestingOrder::order);
  }

  public int size() {
    return ordersById.size();
  }

  public Optional<PlaceOrder> cancel(long orderId) {
    return Optional.ofNullable(ordersById.remove(orderId)).map(RestingOrder::order);
  }

  public OptionalLong bestBidPrice() {
    return ordersById.values().stream().filter(restingOrder -> restingOrder.order().side() == Side.BID)
        .mapToLong(restingOrder -> restingOrder.order().priceTicks()).max();
  }

  public OptionalLong bestAskPrice() {
    return ordersById.values().stream().filter(restingOrder -> restingOrder.order().side() == Side.ASK)
        .mapToLong(restingOrder -> restingOrder.order().priceTicks()).min();
  }

  public Optional<PlaceOrder> bestBid() {
    Optional<PlaceOrder> bestBid = Optional.empty();
    for (RestingOrder restingOrder : ordersById.values()) {
      PlaceOrder order = restingOrder.order();
      if (order.side() == Side.BID && (bestBid.isEmpty() || bestBid.get().priceTicks() < order.priceTicks())) {
        bestBid = Optional.of(order);
      }
    }
    return bestBid;
  }

  public Optional<PlaceOrder> bestAsk() {
    Optional<PlaceOrder> bestAsk = Optional.empty();
    for (RestingOrder restingOrder : ordersById.values()) {
      PlaceOrder order = restingOrder.order();
      if (order.side() == Side.ASK && (bestAsk.isEmpty() || bestAsk.get().priceTicks() > order.priceTicks())) {
        bestAsk = Optional.of(order);
      }
    }
    return bestAsk;
  }

  public OptionalLong remainingLots(long orderId) {
    RestingOrder restingOrder = ordersById.get(orderId);
    if (restingOrder == null) {
      return OptionalLong.empty();
    }

    return OptionalLong.of(ordersById.get(orderId).remainingLots());
  }

  public void fill(long orderId, long quantityLots) {
    RestingOrder restingOrder = ordersById.get(orderId);
    if (restingOrder == null) {
      throw new IllegalArgumentException("Order ID does not exist: " + orderId);
    }

    restingOrder.fill(quantityLots);
    if (restingOrder.remainingLots() == 0L) {
      this.ordersById.remove(orderId);
    }
  }

  public List<OrderSnapshot> snapshot() {
    return ordersById.values().stream().map(order -> new OrderSnapshot(order.order(), order.remainingLots())).toList();
  }
}
