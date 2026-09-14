package dev.sam.exchange.engine;

public record Trade(long incomingOrderId, long restingOrderId, long priceTicks, long quantityLots) {
}
