package dev.sam.exchange.engine;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

public class OrderBook {

  private final Map<Long, RestingOrder> ordersById = new LinkedHashMap<>();
  private final NavigableMap<Long, PriceLevel> bids = new TreeMap<>(Comparator.reverseOrder());
  private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();

  public void add(PlaceOrder order) {
    if (ordersById.containsKey(order.orderId())) {
      throw new IllegalArgumentException("Order ID already exists: " + order.orderId());
    }

    RestingOrder restingOrder = new RestingOrder(order);
    var levels = levelsFor(order.side());

    PriceLevel level = levels.computeIfAbsent(order.priceTicks(), price -> new PriceLevel(order.side(), price));

    level.add(restingOrder);
    ordersById.put(order.orderId(), restingOrder);
  }

  public Optional<PlaceOrder> find(long orderId) {
    return Optional.ofNullable(ordersById.get(orderId)).map(RestingOrder::order);
  }

  public int size() {
    return ordersById.size();
  }

  public Optional<PlaceOrder> cancel(long orderId) {
    RestingOrder restingOrder = ordersById.get(orderId);
    if (restingOrder == null) {
      return Optional.empty();
    }
    remove(restingOrder);
    return Optional.of(restingOrder.order());
  }

  public OptionalLong bestBidPrice() {
    var entry = bids.firstEntry();
    return entry == null ? OptionalLong.empty() : OptionalLong.of(entry.getKey());
  }

  public OptionalLong bestAskPrice() {
    var entry = asks.firstEntry();
    return entry == null ? OptionalLong.empty() : OptionalLong.of(entry.getKey());
  }

  public Optional<PlaceOrder> bestBid() {
    return Optional.ofNullable(bids.firstEntry()).flatMap(entry -> entry.getValue().first()).map(RestingOrder::order);
  }

  public Optional<PlaceOrder> bestAsk() {
    return Optional.ofNullable(asks.firstEntry()).flatMap(entry -> entry.getValue().first()).map(RestingOrder::order);
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
      this.remove(restingOrder);
    }
  }

  public List<OrderSnapshot> snapshot() {
    return ordersById.values().stream().map(order -> new OrderSnapshot(order.order(), order.remainingLots())).toList();
  }

  private NavigableMap<Long, PriceLevel> levelsFor(Side side) {
    return switch (side) {
      case BID -> bids;
      case ASK -> asks;
    };
  }

  private void remove(RestingOrder restingOrder) {
    PlaceOrder order = restingOrder.order();
    var levels = levelsFor(order.side());
    PriceLevel level = levels.get(order.priceTicks());
    if (level != null) {
      level.remove(order.orderId());
      if (level.isEmpty()) {
        levels.remove(order.priceTicks());
      }
    }
    this.ordersById.remove(order.orderId());
  }
}
