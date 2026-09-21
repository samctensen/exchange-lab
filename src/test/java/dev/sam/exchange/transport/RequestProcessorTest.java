package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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
import dev.sam.exchange.persistence.RequestJournal;

class RequestProcessorTest {
  @Test
  void retryOfFilledOrderReturnsOriginalTradesWithoutMatchingAgain(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    RequestJournal journal = new RequestJournal(tempDir.resolve("requests.log"));
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    PlaceOrder bid = new PlaceOrder(2L, Side.BID, 100L, 2L);
    processor.process(request(100L, ask));

    UUID requestId = new UUID(0L, 1L);
    CommandResponse expected = new CommandResponse(requestId,
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L));
    assertEquals(expected, processor.process(new CommandRequest(requestId, bid)));
    assertFalse(book.find(2L).isPresent(), "The filled incoming order is no longer protected by duplicate order IDs");

    // A retry decoded from the wire creates new objects with equal values.
    CommandRequest retry = new CommandRequest(requestId, new PlaceOrder(2L, Side.BID, 100L, 2L));
    assertEquals(expected, processor.process(retry));
    assertEquals(List.of(new OrderSnapshot(ask, 3L)), book.snapshot());
    assertEquals(List.of(ask, bid), journal.readAll().stream().map(CommandRequest::command).toList(),
        "The retry must not append a second bid");
  }

  @Test
  void retryOfCancellationDoesNotCancelANewOrderUsingTheOldOrderId(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    RequestJournal journal = new RequestJournal(tempDir.resolve("requests.log"));
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder original = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    PlaceOrder replacement = new PlaceOrder(1L, Side.ASK, 101L, 7L);
    CancelOrder cancellation = new CancelOrder(1L);
    processor.process(request(100L, original));

    CommandRequest request = new CommandRequest(new UUID(0L, 1L), cancellation);
    CommandResponse expected = new CommandResponse(request.requestId(), new CancelResult(1L, true));
    assertEquals(expected, processor.process(request));
    processor.process(request(101L, replacement));

    assertEquals(expected, processor.process(request));
    assertEquals(List.of(new OrderSnapshot(replacement, 7L)), book.snapshot());
    assertEquals(List.of(original, cancellation, replacement),
        journal.readAll().stream().map(CommandRequest::command).toList());
  }

  @Test
  void differentRequestIdProcessesAnEqualCommandAgain(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    RequestJournal journal = new RequestJournal(tempDir.resolve("requests.log"));
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    PlaceOrder bid = new PlaceOrder(2L, Side.BID, 100L, 2L);
    processor.process(request(100L, ask));
    PlaceResult fill = new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L);
    UUID firstId = new UUID(0L, 1L);
    UUID secondId = new UUID(0L, 2L);

    assertEquals(new CommandResponse(firstId, fill), processor.process(new CommandRequest(firstId, bid)));
    assertEquals(new CommandResponse(secondId, fill), processor.process(new CommandRequest(secondId, bid)));

    assertEquals(List.of(new OrderSnapshot(ask, 1L)), book.snapshot());
    assertEquals(List.of(ask, bid, bid), journal.readAll().stream().map(CommandRequest::command).toList());
  }

  @ParameterizedTest
  @MethodSource("conflictingCommands")
  void conflictingCommandDoesNotChangeStateOrReplaceCachedResponse(EngineCommand conflictingCommand,
      long rejectedOrderId, @TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    RequestJournal journal = new RequestJournal(tempDir.resolve("requests.log"));
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder original = new PlaceOrder(1L, Side.BID, 100L, 10L);
    UUID requestId = new UUID(0L, 1L);
    CommandRequest request = new CommandRequest(requestId, original);
    CommandResponse expected = new CommandResponse(requestId, new PlaceResult(1L, List.of(), 10L));
    assertEquals(expected, processor.process(request));

    CommandRequest conflict = new CommandRequest(requestId, conflictingCommand);
    assertEquals(new CommandResponse(requestId, new RejectResult(rejectedOrderId, RejectReason.REQUEST_ID_CONFLICT)),
        processor.process(conflict));

    assertEquals(expected, processor.process(request));
    assertEquals(List.of(new OrderSnapshot(original, 10L)), book.snapshot());
    assertEquals(List.of(original), journal.readAll().stream().map(CommandRequest::command).toList());
  }

  private static Stream<Arguments> conflictingCommands() {
    return Stream.of(Arguments.of(new PlaceOrder(1L, Side.BID, 100L, 11L), 1L),
        Arguments.of(new PlaceOrder(99L, Side.ASK, 100L, 2L), 99L), Arguments.of(new CancelOrder(1L), 1L));
  }

  @Test
  void retryReturnsOriginalRejectionAfterTheBookChanges(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    RequestJournal journal = new RequestJournal(tempDir.resolve("requests.log"));
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder original = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    processor.process(request(100L, original));
    CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 2L));
    CommandResponse expected = new CommandResponse(request.requestId(),
        new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID));
    assertEquals(expected, processor.process(request));

    // The same command would succeed now, but this request's original rejection must remain stable.
    CancelOrder cancellation = new CancelOrder(1L);
    processor.process(request(102L, cancellation));
    assertEquals(expected, processor.process(request));

    assertEquals(List.of(), book.snapshot());
    assertEquals(List.of(original, request.command(), cancellation),
        journal.readAll().stream().map(CommandRequest::command).toList());
  }

  @Test
  void failedJournalWriteLeavesTheRequestAvailableForRetry(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    Path path = tempDir.resolve("missing-parent/requests.log");
    RequestJournal journal = new RequestJournal(path);
    RequestProcessor processor = new RequestProcessor(new MatchingEngine(book), journal);
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 100L, 10L);
    CommandRequest request = new CommandRequest(new UUID(0L, 1L), order);

    // A missing parent causes a real append failure without changing the book or filling the cache.
    assertThrows(IOException.class, () -> processor.process(request));
    assertEquals(List.of(), book.snapshot());
    assertFalse(Files.exists(path));

    Files.createDirectories(path.getParent());
    assertEquals(new CommandResponse(request.requestId(), new PlaceResult(1L, List.of(), 10L)),
        processor.process(request));
    assertEquals(List.of(new OrderSnapshot(order, 10L)), book.snapshot());
    assertEquals(List.of(request), journal.readAll());
  }

  @Test
  void recoveryRestoresFilledReplyAndRemainingBookWithoutAppending(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    RequestJournal journal = new RequestJournal(path);
    RequestProcessor original = new RequestProcessor(new MatchingEngine(new OrderBook()), journal);
    CommandRequest ask = request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L));
    CommandRequest bid = request(2L, new PlaceOrder(2L, Side.BID, 100L, 2L));
    original.process(ask);
    original.process(bid);
    String beforeRecovery = Files.readString(path);

    RequestProcessor recovered = RequestProcessor.recover(path);
    assertEquals(beforeRecovery, Files.readString(path), "Replaying must not append requests again");
    CommandResponse originalFill = new CommandResponse(bid.requestId(),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L));
    assertEquals(originalFill, recovered.process(bid));
    assertEquals(new CommandResponse(bid.requestId(), new RejectResult(99L, RejectReason.REQUEST_ID_CONFLICT)),
        recovered.process(new CommandRequest(bid.requestId(), new CancelOrder(99L))));
    assertEquals(originalFill, recovered.process(bid));
    assertEquals(beforeRecovery, Files.readString(path), "Retries and UUID conflicts must not append");

    CommandRequest finalFill = request(3L, new PlaceOrder(3L, Side.BID, 100L, 3L));
    CommandResponse expected = new CommandResponse(finalFill.requestId(),
        new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 3L)), 0L));
    assertEquals(expected, recovered.process(finalFill), "Exactly three lots should remain after replay and retry");
    assertEquals(List.of(ask, bid, finalFill), journal.readAll());

    String afterNewRequest = Files.readString(path);
    RequestProcessor recoveredAgain = RequestProcessor.recover(path);
    assertEquals(expected, recoveredAgain.process(finalFill));
    assertEquals(afterNewRequest, Files.readString(path), "A second restart must also leave the journal unchanged");
  }

  @Test
  void recoveryRestoresRejectionEvenAfterItsOrderWasCancelled(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    RequestJournal journal = new RequestJournal(path);
    RequestProcessor original = new RequestProcessor(new MatchingEngine(new OrderBook()), journal);
    CommandRequest ask = request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L));
    CommandRequest duplicate = request(2L, new PlaceOrder(1L, Side.BID, 100L, 2L));
    CommandRequest cancel = request(3L, new CancelOrder(1L));
    original.process(ask);
    CommandResponse rejection = new CommandResponse(duplicate.requestId(),
        new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID));
    assertEquals(rejection, original.process(duplicate));
    original.process(cancel);
    assertEquals(List.of(ask, duplicate, cancel), journal.readAll(),
        "First-time business rejections must be journaled");
    String beforeRecovery = Files.readString(path);

    RequestProcessor recovered = RequestProcessor.recover(path);
    assertEquals(rejection, recovered.process(duplicate));
    assertEquals(beforeRecovery, Files.readString(path));
    CommandRequest probe = request(4L, new CancelOrder(1L));
    assertEquals(new CommandResponse(probe.requestId(), new CancelResult(1L, false)), recovered.process(probe),
        "Replaying or retrying the rejected order must not put it on the book");
    assertEquals(List.of(ask, duplicate, cancel, probe), journal.readAll());
  }

  @Test
  void recoveryRestoresCancellationWithoutCancellingAReplacementOrder(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    RequestJournal journal = new RequestJournal(path);
    RequestProcessor original = new RequestProcessor(new MatchingEngine(new OrderBook()), journal);
    CommandRequest ask = request(1L, new PlaceOrder(1L, Side.ASK, 100L, 5L));
    CommandRequest cancel = request(2L, new CancelOrder(1L));
    CommandRequest replacement = request(3L, new PlaceOrder(1L, Side.ASK, 101L, 7L));
    original.process(ask);
    original.process(cancel);
    original.process(replacement);
    String beforeRecovery = Files.readString(path);

    RequestProcessor recovered = RequestProcessor.recover(path);
    assertEquals(new CommandResponse(cancel.requestId(), new CancelResult(1L, true)), recovered.process(cancel));
    assertEquals(beforeRecovery, Files.readString(path));
    CommandRequest bid = request(4L, new PlaceOrder(2L, Side.BID, 101L, 7L));
    assertEquals(new CommandResponse(bid.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 101L, 7L)), 0L)),
        recovered.process(bid));
    assertEquals(List.of(ask, cancel, replacement, bid), journal.readAll());
  }

  @ParameterizedTest
  @ValueSource(strings = {"PLACE,1,ASK,100,5", "CANCEL,1"})
  void recoveryRejectsRepeatedRequestIdsWithoutChangingTheJournal(String repeatedCommand, @TempDir Path tempDir)
      throws IOException {
    Path path = tempDir.resolve("requests.log");
    String contents = "REQUEST,00000000-0000-0000-0000-000000000001,PLACE,1,ASK,100,5\n"
        + "REQUEST,00000000-0000-0000-0000-000000000001," + repeatedCommand + "\n";
    Files.writeString(path, contents);

    assertThrows(IOException.class, () -> RequestProcessor.recover(path));
    assertEquals(contents, Files.readString(path), "A corrupt journal must not be modified during recovery");
  }

  @Test
  void recoveryFromEmptyJournalAcceptsAndRemembersNewRequests(@TempDir Path tempDir) throws IOException {
    Path path = Files.createFile(tempDir.resolve("requests.log"));
    RequestProcessor processor = RequestProcessor.recover(path);
    CommandRequest request = request(1L, new PlaceOrder(1L, Side.BID, 100L, 10L));
    CommandResponse expected = new CommandResponse(request.requestId(), new PlaceResult(1L, List.of(), 10L));

    assertEquals(expected, processor.process(request));
    assertEquals(expected, processor.process(request));
    assertEquals(List.of(request), new RequestJournal(path).readAll());
  }

  private static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0L, id), command);
  }
}
