package dev.sam.exchange.transport;

record PendingLoggedRequest(CommandRequest request, long endPosition, long deadlineNanos, long admittedNanos,
    long offeredNanos) {
}
