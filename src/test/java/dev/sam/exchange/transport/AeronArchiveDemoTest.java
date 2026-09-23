package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.ReplayResult;
import dev.sam.exchange.engine.ReplayRunner;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;

class AeronArchiveDemoTest {
  @TempDir
  Path directory;

  @Test
  void replaysSbeRequestsAfterArchiveRestartAndRebuildsTheBook() throws IOException {
    PlaceOrder firstAsk = new PlaceOrder(20L, Side.ASK, 100L, 2L);
    PlaceOrder secondAsk = new PlaceOrder(10L, Side.ASK, 100L, 5L);
    List<CommandRequest> requests = List.of(
        new CommandRequest(UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210"), firstAsk),
        new CommandRequest(new UUID(Long.MIN_VALUE, Long.MAX_VALUE), secondAsk),
        new CommandRequest(new UUID(Long.MAX_VALUE, Long.MIN_VALUE), new PlaceOrder(30L, Side.BID, 101L, 4L)),
        new CommandRequest(new UUID(0L, -1L), new CancelOrder(99L)));
    long recordingId = AeronArchiveDemo.record(directory, requests);

    // record() closes Archive and the driver. replay() starts new instances against the saved files.
    List<CommandRequest> replayed = AeronArchiveDemo.replay(directory, recordingId);
    assertEquals(requests, replayed);

    ReplayResult expected = new ReplayResult(
        List.of(new PlaceResult(20L, List.of(), 2L), new PlaceResult(10L, List.of(), 5L),
            new PlaceResult(30L, List.of(new Trade(30L, 20L, 100L, 2L), new Trade(30L, 10L, 100L, 2L)), 0L),
            new CancelResult(99L, false)),
        List.of(new OrderSnapshot(secondAsk, 3L)));

    assertEquals(expected, new ReplayRunner().replay(replayed.stream().map(CommandRequest::command).toList()));
    // Archive saves its previous error buffer on restart. Requests sent on its control stream
    // can still replay correctly, but produce schema errors in that control subscription.
    try (var files = Files.list(directory)) {
      assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith("-error.log")),
          "Archive reported errors while recording the requests");
    }
  }

  @Test
  void selectsTheRequestedRecordingAndPreservesEarlierRecordings() {
    List<CommandRequest> firstRequests = List
        .of(new CommandRequest(new UUID(1L, 2L), new PlaceOrder(1L, Side.BID, 90L, 3L)));
    List<CommandRequest> secondRequests = List.of(new CommandRequest(new UUID(3L, 4L), new CancelOrder(1L)));
    long firstId = AeronArchiveDemo.record(directory, firstRequests);
    long secondId = AeronArchiveDemo.record(directory, secondRequests);

    assertNotEquals(firstId, secondId);
    assertEquals(secondRequests, AeronArchiveDemo.replay(directory, secondId));
    assertEquals(firstRequests, AeronArchiveDemo.replay(directory, firstId));
  }

  @Test
  void replaysAnEmptyRecordingAfterArchiveRestart() {
    long recordingId = AeronArchiveDemo.record(directory, List.of());

    assertEquals(List.of(), AeronArchiveDemo.replay(directory, recordingId));
  }

  @Test
  void preservesRepeatedRequestsAcrossMultipleReplayPolls() {
    List<CommandRequest> requests = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      requests.add(new CommandRequest(new UUID(i, ~((long) i)), new CancelOrder(i)));
    }
    // Archive preserves the stream, including repeated requests. Deduplication belongs to request processing.
    requests.add(7, requests.get(3));
    long recordingId = AeronArchiveDemo.record(directory, requests);

    assertEquals(requests, AeronArchiveDemo.replay(directory, recordingId));
  }
}
