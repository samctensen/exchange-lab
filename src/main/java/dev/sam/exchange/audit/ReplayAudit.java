package dev.sam.exchange.audit;

import java.math.BigInteger;
import java.util.Objects;
import java.util.function.Consumer;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;

/**
 * Applies a request history to a fresh state machine on its owning thread. The supplied state must be
 * used exclusively by this audit. A failed callback can leave partial matching state: discard both
 * state and audit after failure. Later accepts and snapshots fail with the original cause.
 */
public final class ReplayAudit implements Consumer<CommandRequest> {
  private final RequestStateMachine state;
  private final RequestHistoryDigest history = new RequestHistoryDigest();
  private long observedRequests;
  private long uniqueRequests;
  private long duplicateRequests;
  private long conflictingRequests;
  private long placeRequests;
  private long cancelRequests;
  private long businessRejections;
  private long trades;
  private BigInteger executedLots = BigInteger.ZERO;
  private BigInteger notionalTickLots = BigInteger.ZERO;
  private Throwable failure;

  public ReplayAudit(RequestStateMachine state) {
    this.state = Objects.requireNonNull(state, "state must not be null");
  }

  /**
   * Observes one request. Null is a recoverable caller error. Interruption or failure while applying
   * or accounting for a request permanently stops the audit; interruption never clears the flag.
   */
  @Override
  public void accept(CommandRequest request) {
    ensureUsable();
    Objects.requireNonNull(request, "request must not be null");
    try {
      boolean cached = state.hasProcessed(request.requestId());
      var response = state.process(request);
      ensureUsable();
      history.append(request);
      observedRequests++;
      if (response.result() instanceof RejectResult reject && reject.reason() == RejectReason.REQUEST_ID_CONFLICT) {
        conflictingRequests++;
        return;
      }
      if (cached) {
        duplicateRequests++;
        return;
      }
      uniqueRequests++;
      switch (request.command()) {
        case PlaceOrder ignored -> placeRequests++;
        case CancelOrder ignored -> cancelRequests++;
      }
      if (response.result() instanceof RejectResult) {
        businessRejections++;
      }
      if (response.result() instanceof PlaceResult place) {
        for (Trade trade : place.trades()) {
          trades++;
          BigInteger lots = BigInteger.valueOf(trade.quantityLots());
          executedLots = executedLots.add(lots);
          notionalTickLots = notionalTickLots.add(lots.multiply(BigInteger.valueOf(trade.priceTicks())));
        }
      }
    } catch (RuntimeException | Error cause) {
      failure = cause;
      throw cause;
    }
  }

  /** Returns an immutable, nondestructive view of this successful prefix of the history. */
  public ReplayAuditSnapshot snapshot() {
    ensureUsable();
    return new ReplayAuditSnapshot(observedRequests, uniqueRequests, duplicateRequests, conflictingRequests,
        placeRequests, cancelRequests, businessRejections, trades, executedLots, notionalTickLots,
        history.snapshotSha256());
  }

  private void ensureUsable() {
    if (failure != null) {
      throw new IllegalStateException("Replay audit cannot continue after failure", failure);
    }
    if (Thread.currentThread().isInterrupted()) {
      IllegalStateException interrupted = new IllegalStateException("Replay audit interrupted");
      failure = interrupted;
      throw interrupted;
    }
  }
}
