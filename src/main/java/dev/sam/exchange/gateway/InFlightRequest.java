package dev.sam.exchange.gateway;

import org.agrona.ExpandableArrayBuffer;

import dev.sam.exchange.protocol.SbeRequestCodec;

final class InFlightRequest {
  enum Phase {
    OFFERING, AWAITING_REPLY
  }

  final PendingRequest pending;
  final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  final int messageLength;

  Phase phase = Phase.OFFERING;
  int attempts;
  long deadlineNanos;

  public InFlightRequest(PendingRequest pending, SbeRequestCodec codec, long deadlineNanos) {
    this.pending = pending;
    this.messageLength = codec.encode(pending.request(), buffer, 0);
    this.deadlineNanos = deadlineNanos;
  }
}
