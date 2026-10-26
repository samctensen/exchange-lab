package dev.sam.exchange.audit;

import java.math.BigInteger;
import java.util.Objects;
import java.util.function.Consumer;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;

/** Applies a request history to a fresh state machine on its owning thread. */
public final class ReplayAudit implements Consumer<CommandRequest> {
  private final RequestStateMachine state;
  private long observedRequests;
  private long uniqueRequests;
  private long placeRequests;
  private long cancelRequests;
  private long businessRejections;

  public ReplayAudit(RequestStateMachine state) {
    this.state = Objects.requireNonNull(state, "state must not be null");
  }

  @Override
  public void accept(CommandRequest request) {
    Objects.requireNonNull(request, "request must not be null");
    var response = state.process(request);
    observedRequests++;
    uniqueRequests++;
    switch (request.command()) {
      case PlaceOrder ignored -> placeRequests++;
      case CancelOrder ignored -> cancelRequests++;
    }
    if (response.result() instanceof RejectResult) {
      businessRejections++;
    }
  }

  public ReplayAuditSnapshot snapshot() {
    return new ReplayAuditSnapshot(observedRequests, uniqueRequests, 0L, 0L, placeRequests, cancelRequests,
        businessRejections, 0L, BigInteger.ZERO, BigInteger.ZERO, "");
  }
}
