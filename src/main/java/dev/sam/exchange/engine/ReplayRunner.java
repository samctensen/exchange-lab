package dev.sam.exchange.engine;

import java.util.ArrayList;
import java.util.List;

public class ReplayRunner {
  public ReplayResult replay(List<EngineCommand> commands) {
    OrderBook orderBook = new OrderBook();
    MatchingEngine engine = new MatchingEngine(orderBook);
    List<CommandResult> results = new ArrayList<>();

    for (EngineCommand command : commands) {
      CommandResult result = engine.process(command);
      results.add(result);
    }

    List<OrderSnapshot> bookSnapshot = orderBook.snapshot();

    return new ReplayResult(results, bookSnapshot);
  }
}
