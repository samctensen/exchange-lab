package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;

class RequestStateMachineTest {
  private final OrderBook book = new OrderBook();
  private final RequestStateMachine machine = new RequestStateMachine(new MatchingEngine(book));

  @Test
  void retryOfCancelledPlaceRequestDoesNotRecreateTheOrder() {
    CommandRequest place = request(1L, new PlaceOrder(10L, Side.BID, 100L, 10L));
    CommandResponse original = new CommandResponse(place.requestId(), new PlaceResult(10L, List.of(), 10L));
    assertEquals(original, machine.process(place));
    CommandRequest cancel = request(2L, new CancelOrder(10L));
    assertEquals(new CommandResponse(cancel.requestId(), new CancelResult(10L, true)), machine.process(cancel));

    CommandRequest retry = request(1L, new PlaceOrder(10L, Side.BID, 100L, 10L));
    assertEquals(original, machine.process(retry));
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void retryOfFilledOrderReturnsOriginalTradesWithoutMatchingAgain() {
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    machine.process(request(1L, ask));
    CommandRequest bid = request(2L, new PlaceOrder(2L, Side.BID, 100L, 2L));
    CommandResponse expected = new CommandResponse(bid.requestId(),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L));
    assertEquals(expected, machine.process(bid));

    assertEquals(expected, machine.process(request(2L, new PlaceOrder(2L, Side.BID, 100L, 2L))));
    assertEquals(List.of(new OrderSnapshot(ask, 3L)), book.snapshot());
  }

  @ParameterizedTest
  @MethodSource("conflictingCommands")
  void conflictPreservesTheOriginalResponseAndBook(EngineCommand conflictingCommand) {
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 100L, 10L);
    CommandRequest original = request(1L, order);
    CommandResponse expected = new CommandResponse(original.requestId(), new PlaceResult(1L, List.of(), 10L));
    assertEquals(expected, machine.process(original));

    CommandRequest conflict = new CommandRequest(original.requestId(), conflictingCommand);
    assertEquals(
        new CommandResponse(original.requestId(),
            new RejectResult(conflictingCommand.orderId(), RejectReason.REQUEST_ID_CONFLICT)),
        machine.process(conflict));

    assertEquals(expected, machine.process(original));
    assertEquals(List.of(new OrderSnapshot(order, 10L)), book.snapshot());
  }

  static Stream<Arguments> conflictingCommands() {
    return Stream.of(Arguments.of(new PlaceOrder(1L, Side.BID, 100L, 11L)),
        Arguments.of(new PlaceOrder(99L, Side.ASK, 100L, 2L)), Arguments.of(new CancelOrder(1L)));
  }

  @Test
  void retryReturnsOriginalRejectionAfterTheBookChanges() {
    machine.process(request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L)));
    CommandRequest duplicate = request(2L, new PlaceOrder(1L, Side.BID, 100L, 2L));
    CommandResponse rejection = new CommandResponse(duplicate.requestId(),
        new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID));
    assertEquals(rejection, machine.process(duplicate));
    machine.process(request(3L, new CancelOrder(1L)));

    assertEquals(rejection, machine.process(duplicate));
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void newRequestIdProcessesAnEqualCommandAgain() {
    machine.process(request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L)));
    CommandRequest first = request(2L, new CancelOrder(1L));
    CommandRequest second = request(3L, new CancelOrder(1L));

    assertEquals(new CommandResponse(first.requestId(), new CancelResult(1L, true)), machine.process(first));
    assertEquals(new CommandResponse(second.requestId(), new CancelResult(1L, false)), machine.process(second));
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void retryOfCancellationDoesNotCancelAReplacementOrder() {
    machine.process(request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L)));
    CommandRequest cancel = request(2L, new CancelOrder(1L));
    CommandResponse original = new CommandResponse(cancel.requestId(), new CancelResult(1L, true));
    assertEquals(original, machine.process(cancel));
    PlaceOrder replacement = new PlaceOrder(1L, Side.ASK, 101L, 7L);
    machine.process(request(3L, replacement));

    assertEquals(original, machine.process(cancel));
    assertEquals(List.of(new OrderSnapshot(replacement, 7L)), book.snapshot());
  }

  private static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0L, id), command);
  }
}
