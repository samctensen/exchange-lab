package dev.sam.exchange.engine;

import java.util.Objects;

public record RejectResult(long orderId, RejectReason reason) implements CommandResult {
  public RejectResult {
    Objects.requireNonNull(reason, "reason must not be null");
  }
}
