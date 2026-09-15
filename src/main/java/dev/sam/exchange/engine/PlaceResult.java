package dev.sam.exchange.engine;

import java.util.List;

public record PlaceResult(long orderId, List<Trade> trades, long remainingLots) implements CommandResult {

  public PlaceResult {
    trades = List.copyOf(trades);
  }
}
