package dev.sam.exchange.engine;

import java.util.List;

public record ReplayResult(List<CommandResult> results, List<OrderSnapshot> bookSnapshot) {
  public ReplayResult {
    results = List.copyOf(results);
    bookSnapshot = List.copyOf(bookSnapshot);
  }
}
