package dev.sam.exchange.engine;

public sealed interface CommandResult permits PlaceResult, CancelResult {
}
