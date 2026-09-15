package dev.sam.exchange.engine;

public sealed interface EngineCommand permits PlaceOrder, CancelOrder {
}
