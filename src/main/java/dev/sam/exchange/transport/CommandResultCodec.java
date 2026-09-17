package dev.sam.exchange.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Trade;

public class CommandResultCodec {

  public String encode(CommandResult result) {
    return switch (result) {
      case CancelResult cancel -> "CANCEL_RESULT," + cancel.orderId() + "," + cancel.cancelled();
      case PlaceResult order -> {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add("PLACE_RESULT");
        joiner.add(Long.toString(order.orderId()));
        joiner.add(Long.toString(order.remainingLots()));
        joiner.add(Integer.toString(order.trades().size()));

        for (Trade trade : order.trades()) {
          joiner.add(Long.toString(trade.incomingOrderId()));
          joiner.add(Long.toString(trade.restingOrderId()));
          joiner.add(Long.toString(trade.priceTicks()));
          joiner.add(Long.toString(trade.quantityLots()));
        }

        yield joiner.toString();
      }
    };
  }

  public CommandResult decode(String line) {
    String[] parts = line.split(",", -1);
    return switch (parts[0]) {
      case "CANCEL_RESULT" -> {
        if (parts.length != 3) {
          throw new IllegalArgumentException("Invalid CANCEL command: " + line);
        }
        if (!parts[2].equals("true") && !parts[2].equals("false")) {
          throw new IllegalArgumentException("Invalid CANCEL command: " + line);
        }
        yield new CancelResult(Long.parseLong(parts[1]), Boolean.parseBoolean(parts[2]));
      }
      case "PLACE_RESULT" -> {
        if (parts.length < 4) {
          throw new IllegalArgumentException("Invalid PLACE command: " + line);
        }
        int numTrades = Integer.parseInt(parts[3]);
        if (numTrades < 0 || parts.length != 4L + 4L * numTrades) {
          throw new IllegalArgumentException("Invalid PLACE command: " + line);
        }
        List<Trade> trades = new ArrayList<>();
        for (int i = 4; i < parts.length; i += 4) {
          trades.add(new Trade(Long.parseLong(parts[i]), Long.parseLong(parts[i + 1]), Long.parseLong(parts[i + 2]),
              Long.parseLong(parts[i + 3])));
        }
        yield new PlaceResult(Long.parseLong(parts[1]), trades, Long.parseLong(parts[2]));
      }
      default -> throw new IllegalArgumentException("Unknown command: " + parts[0]);
    };
  }
}
