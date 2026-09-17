package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.ReplayResult;
import dev.sam.exchange.engine.ReplayRunner;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;

class CommandJournalTest {

  @Test
  void createsJournalAndAppendsCommandsInOrder(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    CommandJournal journal = new CommandJournal(path);

    journal.append(new PlaceOrder(1L, Side.BID, 100L, 10L));
    journal.append(new CancelOrder(1L));

    assertEquals(List.of("PLACE,1,BID,100,10", "CANCEL,1"), Files.readAllLines(path));
  }

  @Test
  void replaysCommandsLoadedFromJournal(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    CommandJournal journal = new CommandJournal(path);
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    List<EngineCommand> commands = List.of(ask, new PlaceOrder(2L, Side.BID, 100L, 2L), new CancelOrder(99L));

    for (EngineCommand command : commands) {
      journal.append(command);
    }

    List<EngineCommand> restoredCommands = new CommandJournal(path).readAll();
    ReplayResult expected = new ReplayResult(List.of(new PlaceResult(1L, List.of(), 5L),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L), new CancelResult(99L, false)),
        List.of(new OrderSnapshot(ask, 3L)));

    assertEquals(commands, restoredCommands);
    assertEquals(expected, new ReplayRunner().replay(restoredCommands));
  }

  @Test
  void rejectsUnterminatedFinalCommand(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("commands.log");
    String history = "CANCEL,99";
    Files.writeString(path, history);
    CommandJournal journal = new CommandJournal(path);

    assertThrows(IOException.class, journal::readAll);

    assertEquals(history, Files.readString(path));
  }
}
