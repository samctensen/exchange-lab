package dev.sam.exchange.gateway;

import java.util.Locale;

/** Opt-in aggregate diagnostics. Snapshots are thread-safe; final reports include warmup and shutdown drain. */
final class GatewayDiagnostics {
  private long accepted;
  private long queueFull;
  private long notRunning;
  private long completed;
  private long businessRejected;
  private long failed;
  private long unmatchedResponses;
  private long activated;
  private long queueWaitNanos;
  private long maxQueueWaitNanos;
  private int queueHighWater;
  private int activeHighWater;
  private long offers;
  private long retries;
  private long offerTimeouts;
  private long replyTimeouts;

  synchronized void accepted(int observedQueueSize) {
    accepted++;
    queueHighWater = Math.max(queueHighWater, observedQueueSize);
  }

  synchronized void queueFull() {
    queueFull++;
  }

  synchronized void notRunning() {
    notRunning++;
  }

  // Definitive engine results, including business rejection; independent of caller cancellation.
  synchronized void completed(boolean rejected) {
    completed++;
    if (rejected)
      businessRejected++;
  }

  // Accepted requests with a terminal processing failure; retry timeouts are not terminal.
  synchronized void failed() {
    failed++;
  }

  synchronized void unmatchedResponse() {
    unmatchedResponses++;
  }

  synchronized void activated(long waitNanos, int activeCount) {
    activated++;
    queueWaitNanos += waitNanos;
    maxQueueWaitNanos = Math.max(maxQueueWaitNanos, waitNanos);
    activeHighWater = Math.max(activeHighWater, activeCount);
  }

  synchronized void offered(boolean retry) {
    offers++;
    if (retry)
      retries++;
  }

  synchronized void timedOut(boolean awaitingReply) {
    if (awaitingReply)
      replyTimeouts++;
    else
      offerTimeouts++;
  }

  synchronized Snapshot snapshot() {
    return new Snapshot(accepted, queueFull, notRunning, completed, businessRejected, failed, unmatchedResponses,
        activated, queueWaitNanos, maxQueueWaitNanos, queueHighWater, activeHighWater, offers, retries, offerTimeouts,
        replyTimeouts);
  }

  String summarize() {
    return snapshot().summarize();
  }

  record Snapshot(long accepted, long queueFull, long notRunning, long completed, long businessRejected, long failed,
      long unmatchedResponses, long activated, long queueWaitNanos, long maxQueueWaitNanos, int queueHighWater,
      int activeHighWater, long offers, long retries, long offerTimeouts, long replyTimeouts) {
    String summarize() {
      return String.format(Locale.ROOT, "Gateway diagnostics: lifetime (includes warmup and shutdown drain)%n"
          + "Gateway accepted: %d%nGateway queue full: %d%nGateway not running: %d%nGateway activated: %d%n"
          + "Gateway completed: %d%nGateway business rejected: %d%nGateway failed: %d%nGateway unmatched responses: %d%n"
          + "Gateway queue high-water (observed): %d%nGateway active high-water: %d%n"
          + "Gateway queue wait mean: %.3f us%nGateway queue wait max: %.3f us%n"
          + "Gateway successful offers: %d%nGateway retries: %d%n"
          + "Gateway offer timeouts: %d%nGateway reply timeouts: %d%n", accepted, queueFull, notRunning, activated,
          completed, businessRejected, failed, unmatchedResponses, queueHighWater, activeHighWater,
          activated == 0 ? 0 : queueWaitNanos / (activated * 1000.0), maxQueueWaitNanos / 1000.0, offers, retries,
          offerTimeouts, replyTimeouts);
    }
  }
}
