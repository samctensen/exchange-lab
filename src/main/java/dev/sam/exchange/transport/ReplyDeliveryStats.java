package dev.sam.exchange.transport;

/** Written by the engine agent; read after its runner has stopped. Counts deliveries, not executions. */
final class ReplyDeliveryStats {
  enum DropReason {
    EXPIRED, CAPACITY, DISCONNECTED, UNAVAILABLE, UNENCODABLE
  }

  private long sent;
  private final long[] dropped = new long[DropReason.values().length];

  void sent() {
    sent++;
  }

  void dropped(DropReason reason, int count) {
    dropped[reason.ordinal()] += count;
  }

  long sentCount() {
    return sent;
  }

  long droppedCount(DropReason reason) {
    return dropped[reason.ordinal()];
  }

  String summarize() {
    return "Reply delivery: offered=" + sent + ", expired=" + droppedCount(DropReason.EXPIRED) + ", capacity="
        + droppedCount(DropReason.CAPACITY) + ", disconnected=" + droppedCount(DropReason.DISCONNECTED)
        + ", unavailable=" + droppedCount(DropReason.UNAVAILABLE) + ", unencodable="
        + droppedCount(DropReason.UNENCODABLE) + System.lineSeparator();
  }
}
