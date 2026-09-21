package dev.sam.exchange.transport;

import java.util.UUID;

public class CommandResponseCodec {
  private final CommandResultCodec resultCodec = new CommandResultCodec();

  public String encode(CommandResponse response) {
    return "RESPONSE," + response.requestId() + "," + resultCodec.encode(response.result());
  }

  public CommandResponse decode(String line) {
    String[] parts = line.split(",", 3);
    return switch (parts[0]) {
      case "RESPONSE" -> {
        if (parts.length < 3) {
          throw new IllegalArgumentException("Invalid RESPONSE message: " + line);
        }
        yield new CommandResponse(UUID.fromString(parts[1]), resultCodec.decode(parts[2]));
      }
      default -> throw new IllegalArgumentException("Unknown message type: " + parts[0]);
    };
  }
}
