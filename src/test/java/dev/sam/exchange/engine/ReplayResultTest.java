package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class ReplayResultTest {

  @Test
  void retainsResultsAndSnapshotWhenSourceListsChange() {
    CommandResult commandResult = new CancelResult(99L, false);
    OrderSnapshot snapshot = new OrderSnapshot(new PlaceOrder(1L, Side.ASK, 100L, 5L), 3L);
    List<CommandResult> results = new ArrayList<>(List.of(commandResult));
    List<OrderSnapshot> bookSnapshot = new ArrayList<>(List.of(snapshot));
    ReplayResult replayResult = new ReplayResult(results, bookSnapshot);

    results.clear();
    bookSnapshot.clear();

    assertEquals(List.of(commandResult), replayResult.results());
    assertEquals(List.of(snapshot), replayResult.bookSnapshot());
  }

  @Test
  void doesNotAllowResultsOrSnapshotToBeModifiedThroughResult() {
    CommandResult commandResult = new CancelResult(99L, false);
    OrderSnapshot snapshot = new OrderSnapshot(new PlaceOrder(1L, Side.ASK, 100L, 5L), 3L);
    List<CommandResult> results = new ArrayList<>(List.of(commandResult));
    List<OrderSnapshot> bookSnapshot = new ArrayList<>(List.of(snapshot));
    ReplayResult replayResult = new ReplayResult(results, bookSnapshot);

    assertThrows(UnsupportedOperationException.class, () -> replayResult.results().clear());
    assertThrows(UnsupportedOperationException.class, () -> replayResult.bookSnapshot().clear());
    assertEquals(List.of(commandResult), replayResult.results());
    assertEquals(List.of(snapshot), replayResult.bookSnapshot());
  }
}
