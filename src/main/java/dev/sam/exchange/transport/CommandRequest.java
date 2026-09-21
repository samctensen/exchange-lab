package dev.sam.exchange.transport;

import java.util.Objects;
import java.util.UUID;

import dev.sam.exchange.engine.EngineCommand;

public record CommandRequest(UUID requestId, EngineCommand command) {
  public CommandRequest {
    Objects.requireNonNull(requestId, "requestId must not be null");
    Objects.requireNonNull(command, "command must not be null");
  }
}
