package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import dev.sam.exchange.transport.RequestStateMachine;

class ReplayAuditFailureTest {
  private final OrderBook book = new OrderBook();
  private final CommandRequest place = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(10L, Side.ASK, 100L, 5L));
  private final CommandRequest cancel = new CommandRequest(new UUID(0L, 3L), new CancelOrder(10L));

  @Test
  void invalidNullInputDoesNotAdvanceOrPoisonTheAudit() {
    ReplayAudit audit = new ReplayAudit(new RequestStateMachine(new MatchingEngine(book)));
    audit.accept(place);
    ReplayAuditSnapshot before = audit.snapshot();

    assertThrows(NullPointerException.class, () -> audit.accept(null));
    assertEquals(before, audit.snapshot());
    audit.accept(cancel);

    assertEquals(2L, audit.snapshot().observedRequests());
    assertEquals(List.of(), book.snapshot());
  }

  @ParameterizedTest
  @MethodSource("processingFailures")
  void processingFailureIsRethrownAndPermanentlyPreventsFurtherAuditWork(Throwable failure) {
    RequestStateMachine state = new RequestStateMachine(new MatchingEngine(book)) {
      @Override
      public CommandResponse process(CommandRequest request) {
        CommandResponse response = super.process(request);
        if (request.requestId().getLeastSignificantBits() == 2L) {
          if (failure instanceof RuntimeException exception) {
            throw exception;
          }
          throw (Error) failure;
        }
        return response;
      }
    };
    ReplayAudit audit = new ReplayAudit(state);
    audit.accept(place);
    CommandRequest bid = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(20L, Side.BID, 100L, 2L));

    assertSame(failure, assertThrows(failure.getClass(), () -> audit.accept(bid)));
    assertSame(failure, assertThrows(IllegalStateException.class, audit::snapshot).getCause());
    assertSame(failure, assertThrows(IllegalStateException.class, () -> audit.accept(cancel)).getCause());
    assertSame(failure, assertThrows(IllegalStateException.class, audit::snapshot).getCause());
    // Real matching completed before the injected failure; the later cancellation must never run.
    assertEquals(3L, book.remainingLots(10L).orElseThrow());
  }

  static Stream<Throwable> processingFailures() {
    return Stream.of(new IllegalArgumentException("callback failed after matching"),
        new AssertionError("callback failed after matching"));
  }

  @Test
  void interruptionBeforeAcceptPreservesTheFlagAndPreventsExecutionAndLaterSnapshots() {
    ReplayAudit audit = new ReplayAudit(new RequestStateMachine(new MatchingEngine(book)));
    IllegalStateException interruption;
    Thread.currentThread().interrupt();
    try {
      interruption = assertThrows(IllegalStateException.class, () -> audit.accept(place));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }

    assertSame(interruption, assertThrows(IllegalStateException.class, audit::snapshot).getCause());
    assertSame(interruption, assertThrows(IllegalStateException.class, () -> audit.accept(place)).getCause());
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void interruptionDuringProcessingPreventsASuccessSnapshotEvenAfterTheFlagIsCleared() {
    RequestStateMachine state = new RequestStateMachine(new MatchingEngine(book)) {
      @Override
      public CommandResponse process(CommandRequest request) {
        CommandResponse response = super.process(request);
        Thread.currentThread().interrupt();
        return response;
      }
    };
    ReplayAudit audit = new ReplayAudit(state);
    IllegalStateException interruption;
    try {
      interruption = assertThrows(IllegalStateException.class, () -> audit.accept(place));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }

    assertSame(interruption, assertThrows(IllegalStateException.class, audit::snapshot).getCause());
    assertSame(interruption, assertThrows(IllegalStateException.class, () -> audit.accept(cancel)).getCause());
    assertEquals(5L, book.remainingLots(10L).orElseThrow());
  }

  @Test
  void interruptionAtSnapshotPreventsPublishingTheCompletedPrefix() {
    ReplayAudit audit = new ReplayAudit(new RequestStateMachine(new MatchingEngine(book)));
    audit.accept(place);
    IllegalStateException interruption;
    Thread.currentThread().interrupt();
    try {
      interruption = assertThrows(IllegalStateException.class, audit::snapshot);
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }

    assertSame(interruption, assertThrows(IllegalStateException.class, audit::snapshot).getCause());
    assertEquals(5L, book.remainingLots(10L).orElseThrow());
  }
}
