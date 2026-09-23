package dev.sam.exchange.protocol;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;

public class CommandCodec {

  public String encode(EngineCommand command) {
    return switch (command) {
      case CancelOrder cancel -> "CANCEL," + cancel.orderId();
      case PlaceOrder order ->
        "PLACE," + order.orderId() + "," + order.side() + "," + order.priceTicks() + "," + order.quantityLots();
    };
  }

  public EngineCommand decode(String line) {
    String[] parts = line.split(",", -1);
    return switch (parts[0]) {
      case "CANCEL" -> {
        if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid CANCEL command: " + line);
        }
        yield new CancelOrder(Long.parseLong(parts[1]));
      }
      case "PLACE" -> {
        if (parts.length != 5) {
          throw new IllegalArgumentException("Invalid PLACE command: " + line);
        }
        yield new PlaceOrder(Long.parseLong(parts[1]), Side.valueOf(parts[2]), Long.parseLong(parts[3]),
            Long.parseLong(parts[4]));
      }
      default -> throw new IllegalArgumentException("Unknown command: " + parts[0]);
    };
  }
}
