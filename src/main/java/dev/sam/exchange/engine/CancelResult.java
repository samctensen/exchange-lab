package dev.sam.exchange.engine;

public record CancelResult(long orderId, boolean cancelled) implements CommandResult {
}
