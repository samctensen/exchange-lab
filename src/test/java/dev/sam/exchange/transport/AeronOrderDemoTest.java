package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;

class AeronOrderDemoTest {
  @Test
  @Timeout(30)
  void transportsCommandsThroughMatchingEngine() {
    OrderBook book = new OrderBook();
    MatchingEngine engine = new MatchingEngine(book);

    PlaceOrder bid = new PlaceOrder(1L, Side.BID, 100L, 10L);
    PlaceOrder ask = new PlaceOrder(2L, Side.ASK, 99L, 4L);
    List<EngineCommand> commands = List.of(bid, ask);

    List<CommandResult> results = AeronOrderDemo.run(commands, engine);

    // The incoming ask trades at the resting bid's price: 100, rather than 99.
    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        results);

    // Selling four lots against the ten-lot bid leaves six lots on the book.
    assertEquals(List.of(new OrderSnapshot(bid, 6L)), book.snapshot());

  }
}
