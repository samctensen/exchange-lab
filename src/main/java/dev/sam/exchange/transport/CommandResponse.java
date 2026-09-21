package dev.sam.exchange.transport;

import java.util.Objects;
import java.util.UUID;

import dev.sam.exchange.engine.CommandResult;

public record CommandResponse(UUID requestId, CommandResult result) {
  public CommandResponse {
    Objects.requireNonNull(requestId, "requestId must not be null");
    Objects.requireNonNull(result, "result must not be null");
  }
}
