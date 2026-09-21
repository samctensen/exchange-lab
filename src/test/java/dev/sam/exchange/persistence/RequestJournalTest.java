package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;

class RequestJournalTest {
  @Test
  void createsJournalAndPreservesEntriesWhenReopenedForAppend(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    RequestJournal journal = new RequestJournal(path);
    journal.append(new CommandRequest(new UUID(0L, 1L), new PlaceOrder(7L, Side.BID, 100L, 3L)));

    // A new journal instance must append to the existing file, including a final newline for each request.
    new RequestJournal(path).append(new CommandRequest(new UUID(0L, 2L), new CancelOrder(7L)));

    assertEquals("REQUEST,00000000-0000-0000-0000-000000000001,PLACE,7,BID,100,3\n"
        + "REQUEST,00000000-0000-0000-0000-000000000002,CANCEL,7\n", Files.readString(path));
  }

  @Test
  void readsRequestIdsAndCommandsInFileOrder(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    // Literal input checks reading independently of the journal's append method.
    String history = "REQUEST,00000000-0000-0000-0000-000000000002,CANCEL,99\n"
        + "REQUEST,00000000-0000-0000-0000-000000000001,PLACE,7,BID,100,3\n";
    Files.writeString(path, history);

    assertEquals(
        List.of(new CommandRequest(new UUID(0L, 2L), new CancelOrder(99L)),
            new CommandRequest(new UUID(0L, 1L), new PlaceOrder(7L, Side.BID, 100L, 3L))),
        new RequestJournal(path).readAll());
    assertEquals(history, Files.readString(path));
  }

  @Test
  void readsAnEmptyJournal(@TempDir Path tempDir) throws IOException {
    Path path = Files.createFile(tempDir.resolve("requests.log"));

    assertEquals(List.of(), new RequestJournal(path).readAll());
  }

  @Test
  void missingJournalFailsWithoutCreatingAnEmptyReplacement(@TempDir Path tempDir) {
    Path path = tempDir.resolve("missing.log");

    assertThrows(IOException.class, new RequestJournal(path)::readAll);
    assertFalse(Files.exists(path));
  }

  @ParameterizedTest
  @ValueSource(strings = {"REQUEST,00000000-0000-0000-0000-000000000001,CANCEL,7",
      "REQUEST,00000000-0000-0000-0000-000000000001,CANCEL,7\nREQUEST,"})
  void rejectsUnterminatedFinalRequestWithoutChangingTheFile(String history, @TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    Files.writeString(path, history);

    assertThrows(IOException.class, new RequestJournal(path)::readAll);
    assertEquals(history, Files.readString(path));
  }

  @Test
  void failsOnMalformedTerminatedEntryInsteadOfSkippingIt(@TempDir Path tempDir) throws IOException {
    Path path = tempDir.resolve("requests.log");
    String history = "REQUEST,00000000-0000-0000-0000-000000000001,CANCEL,7\n" + "REQUEST,not-a-uuid,CANCEL,8\n";
    Files.writeString(path, history);

    assertThrows(IllegalArgumentException.class, new RequestJournal(path)::readAll);
    assertEquals(history, Files.readString(path));
  }

  @Test
  void propagatesAppendFailureWhenTheParentDirectoryIsMissing(@TempDir Path tempDir) {
    Path path = tempDir.resolve("missing-parent/requests.log");
    RequestJournal journal = new RequestJournal(path);
    CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));

    assertThrows(IOException.class, () -> journal.append(request));
    assertFalse(Files.exists(path));
  }
}
