package dev.sam.exchange.engine;

public record RejectResult(long orderId, RejectReason reason) implements CommandResult {
}
