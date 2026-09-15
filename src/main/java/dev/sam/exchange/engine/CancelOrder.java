package dev.sam.exchange.engine;

public record CancelOrder(long orderId) implements EngineCommand {
}
