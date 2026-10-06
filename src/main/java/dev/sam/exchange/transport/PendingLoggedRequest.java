package dev.sam.exchange.transport;

record PendingLoggedRequest(RoutedRequest routedRequest, long endPosition, long deadlineNanos, long admittedNanos,
    long offeredNanos) {
}
