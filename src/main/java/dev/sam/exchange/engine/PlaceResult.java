package dev.sam.exchange.engine;

import java.util.List;

public record PlaceResult(long orderId, List<Trade> trades, long remainingLots) implements CommandResult {

  public PlaceResult {
    if (remainingLots < 0) {
      throw new IllegalArgumentException("Remaining lots must not be negative");
    }
    trades = List.copyOf(trades);
  }
}
