package dev.sam.exchange.engine;

import java.util.List;

/** Immutable price-level projections in the order provided by the book. */
public record BookDepth(List<PriceLevelSnapshot> bids, List<PriceLevelSnapshot> asks) {
  public BookDepth {
    bids = List.copyOf(bids);
    asks = List.copyOf(asks);
  }
}
