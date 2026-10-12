package dev.sam.exchange.transport;

import java.util.UUID;

import dev.sam.exchange.protocol.CommandCodec;

public class CommandRequestCodec {
  private final CommandCodec commandCodec = new CommandCodec();

  public String encode(CommandRequest request) {
    return "REQUEST," + request.requestId() + "," + commandCodec.encode(request.command());
  }

  public CommandRequest decode(String line) {
    String[] parts = line.split(",", 3);
    return switch (parts[0]) {
      case "REQUEST" -> {
        if (parts.length < 3) {
          throw new IllegalArgumentException("Invalid REQUEST message: " + line);
        }
        UUID requestId = UUID.fromString(parts[1]);
        if (!requestId.toString().equalsIgnoreCase(parts[1])) {
          throw new IllegalArgumentException("requestId must be a full UUID");
        }
        yield new CommandRequest(requestId, commandCodec.decode(parts[2]));
      }
      default -> throw new IllegalArgumentException("Unknown message type: " + parts[0]);
    };
  }
}
