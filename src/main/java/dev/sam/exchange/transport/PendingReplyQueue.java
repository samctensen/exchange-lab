package dev.sam.exchange.transport;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

final class PendingReplyQueue {
  private record Entry(PendingReply reply, int encodedLength) {
  }

  private final Deque<Entry> entries = new ArrayDeque<>();
  private final int maxReplies;
  private final long maxBytes;
  private long queuedBytes;

  public PendingReplyQueue(int maxReplies, long maxBytes) {
    if (maxReplies <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("maxReplies and maxBytes must be positive");
    }
    this.maxReplies = maxReplies;
    this.maxBytes = maxBytes;
  }

  public boolean offer(PendingReply reply, int encodedLength) {
    Objects.requireNonNull(reply, "reply");
    if (encodedLength <= 0) {
      throw new IllegalArgumentException("encodedLength must be positive");
    }
    if (this.entries.size() >= this.maxReplies || encodedLength > this.maxBytes - this.queuedBytes) {
      return false;
    }

    this.entries.offer(new Entry(reply, encodedLength));
    this.queuedBytes += encodedLength;
    return true;
  }

  public PendingReply peek() {
    if (this.entries.isEmpty()) {
      return null;
    }
    return this.entries.peek().reply();
  }

  public PendingReply poll() {
    Entry entry = this.entries.poll();
    if (entry == null) {
      return null;
    }
    this.queuedBytes -= entry.encodedLength();
    return entry.reply();
  }

  public void clear() {
    this.entries.clear();
    this.queuedBytes = 0;
  }

  public int size() {
    return this.entries.size();
  }

  public long queuedBytes() {
    return this.queuedBytes;
  }
}
