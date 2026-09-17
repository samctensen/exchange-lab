package dev.sam.exchange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.persistence.CommandJournal;

class JournaledEngineTest {

  @Test
  void recordsAcceptedCommandsAndReturnsEngineResults(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    CommandJournal journal = new CommandJournal(tempDir.resolve("commands.log"));
    JournaledEngine engine = new JournaledEngine(new MatchingEngine(book), journal);
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    PlaceOrder bid = new PlaceOrder(2L, Side.BID, 100L, 2L);
    CancelOrder cancellation = new CancelOrder(1L);
    CancelOrder unknownCancellation = new CancelOrder(99L);

    assertEquals(new PlaceResult(1L, List.of(), 5L), engine.process(ask));
    assertEquals(new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L), engine.process(bid));
    assertEquals(List.of(new OrderSnapshot(ask, 3L)), book.snapshot());
    assertEquals(new CancelResult(1L, true), engine.process(cancellation));
    assertEquals(new CancelResult(99L, false), engine.process(unknownCancellation));
    assertEquals(List.of(ask, bid, cancellation, unknownCancellation), journal.readAll());
    assertEquals(List.of(), book.snapshot());
  }

  @Test
  void rejectsDuplicateOrderWithoutChangingJournalOrBook(@TempDir Path tempDir) throws IOException {
    OrderBook book = new OrderBook();
    Path path = tempDir.resolve("commands.log");
    CommandJournal journal = new CommandJournal(path);
    JournaledEngine engine = new JournaledEngine(new MatchingEngine(book), journal);
    engine.process(new PlaceOrder(1L, Side.ASK, 100L, 5L));
    String journalBefore = Files.readString(path);
    List<OrderSnapshot> bookBefore = book.snapshot();

    assertThrows(IllegalArgumentException.class, () -> engine.process(new PlaceOrder(1L, Side.BID, 100L, 2L)));

    assertEquals(journalBefore, Files.readString(path));
    assertEquals(bookBefore, book.snapshot());
  }

  @Test
  void failedJournalWriteDoesNotPlaceOrMatchOrder(@TempDir Path tempDir) {
    OrderBook book = new OrderBook();
    book.add(new PlaceOrder(1L, Side.ASK, 100L, 5L));
    List<OrderSnapshot> bookBefore = book.snapshot();
    // A directory cannot be opened as a journal file.
    CommandJournal journal = new CommandJournal(tempDir);
    JournaledEngine engine = new JournaledEngine(new MatchingEngine(book), journal);

    assertThrows(IOException.class, () -> engine.process(new PlaceOrder(2L, Side.BID, 100L, 2L)));

    assertEquals(bookBefore, book.snapshot());
  }

  @Test
  void failedJournalWriteDoesNotCancelOrder(@TempDir Path tempDir) {
    OrderBook book = new OrderBook();
    book.add(new PlaceOrder(1L, Side.ASK, 100L, 5L));
    List<OrderSnapshot> bookBefore = book.snapshot();
    CommandJournal journal = new CommandJournal(tempDir);
    JournaledEngine engine = new JournaledEngine(new MatchingEngine(book), journal);

    assertThrows(IOException.class, () -> engine.process(new CancelOrder(1L)));

    assertEquals(bookBefore, book.snapshot());
  }

  @Test
  void recoversRemainingQuantityAndAppendsOnlyNewCommands(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    CommandJournal journal = new CommandJournal(path);
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    PlaceOrder earlierBid = new PlaceOrder(2L, Side.BID, 100L, 2L);
    journal.append(ask);
    journal.append(earlierBid);
    String journalBefore = Files.readString(path);

    JournaledEngine recovered = JournaledEngine.recover(path);

    assertEquals(journalBefore, Files.readString(path));

    PlaceOrder newBid = new PlaceOrder(3L, Side.BID, 100L, 4L);
    assertEquals(new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 3L)), 1L), recovered.process(newBid));
    assertEquals(List.of(ask, earlierBid, newBid), journal.readAll());
  }

  @Test
  void recoversEmptyJournal(@TempDir Path tempDir) throws IOException {
    Path path = Files.createFile(tempDir.resolve("commands.log"));

    JournaledEngine recovered = JournaledEngine.recover(path);
    PlaceOrder order = new PlaceOrder(1L, Side.ASK, 100L, 5L);

    assertEquals(new PlaceResult(1L, List.of(), 5L), recovered.process(order));
    assertEquals(List.of(order), new CommandJournal(path).readAll());
  }

  @Test
  void failsRecoveryWhenJournalDoesNotExist(@TempDir Path tempDir) {
    Path path = tempDir.resolve("missing.log");

    assertThrows(IOException.class, () -> JournaledEngine.recover(path));

    assertFalse(Files.exists(path));
  }

  @Test
  void rejectsMalformedJournalWithoutChangingIt(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    String history = "PLACE,1,ASK,100,5\nUNKNOWN,2\n";
    Files.writeString(path, history);

    assertThrows(IllegalArgumentException.class, () -> JournaledEngine.recover(path));

    assertEquals(history, Files.readString(path));
  }

  @Test
  void rejectsDuplicateOrdersInJournalWithoutChangingIt(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    String history = "PLACE,1,ASK,100,5\nPLACE,1,BID,100,2\n";
    Files.writeString(path, history);

    assertThrows(IllegalArgumentException.class, () -> JournaledEngine.recover(path));

    assertEquals(history, Files.readString(path));
  }

  @Test
  void rejectsUnterminatedJournalWithoutChangingIt(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    String history = "CANCEL,99";
    Files.writeString(path, history);

    assertThrows(IOException.class, () -> JournaledEngine.recover(path));

    assertEquals(history, Files.readString(path));
  }
}
