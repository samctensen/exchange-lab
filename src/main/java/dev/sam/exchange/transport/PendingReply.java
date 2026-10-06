package dev.sam.exchange.transport;

import java.util.Objects;

record PendingReply(CommandResponse response, long deadlineNanos, PendingLoggedRequest timedRequest, long recordedNanos,
    long preparedNanos) {

  PendingReply {
    Objects.requireNonNull(response, "response");
  }
}
