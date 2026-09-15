package dev.sam.exchange.engine;

public record OrderSnapshot(PlaceOrder order, long remainingLots) {
}
