package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

class ReplayRunnerTest {

  @Test
  void replaysSameCommandsToExpectedResultsAndSnapshot() {
    PlaceOrder ask = new PlaceOrder(1L, Side.ASK, 100L, 5L);
    List<EngineCommand> commands = List.of(ask, new PlaceOrder(2L, Side.BID, 100L, 2L), new CancelOrder(99L));
    ReplayRunner replayRunner = new ReplayRunner();

    ReplayResult result1 = replayRunner.replay(commands);
    ReplayResult result2 = replayRunner.replay(commands);

    ReplayResult expected = new ReplayResult(List.of(new PlaceResult(1L, List.of(), 5L),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L), new CancelResult(99L, false)),
        List.of(new OrderSnapshot(ask, 3L)));
    assertEquals(expected, result1);
    assertEquals(result1, result2);
  }

  @Test
  void emptyReplayDoesNotCarryOverEarlierState() {
    ReplayRunner replayRunner = new ReplayRunner();
    PlaceOrder order = new PlaceOrder(7L, Side.BID, 100L, 2L);
    ReplayResult earlierResult = replayRunner.replay(List.of(order));

    assertEquals(new ReplayResult(List.of(), List.of()), replayRunner.replay(List.of()));
    assertEquals(new ReplayResult(List.of(new PlaceResult(7L, List.of(), 2L)), List.of(new OrderSnapshot(order, 2L))),
        earlierResult);
  }

  @Test
  void preservesCommandOrderAndArrivalPriorityDuringReplay() {
    List<EngineCommand> commands = List.of(new CancelOrder(99L), new PlaceOrder(20L, Side.ASK, 100L, 2L),
        new PlaceOrder(10L, Side.ASK, 100L, 3L), new PlaceOrder(30L, Side.BID, 100L, 4L), new CancelOrder(10L));

    ReplayResult result = new ReplayRunner().replay(commands);

    ReplayResult expected = new ReplayResult(
        List.of(new CancelResult(99L, false), new PlaceResult(20L, List.of(), 2L), new PlaceResult(10L, List.of(), 3L),
            new PlaceResult(30L, List.of(new Trade(30L, 20L, 100L, 2L), new Trade(30L, 10L, 100L, 2L)), 0L),
            new CancelResult(10L, true)),
        List.of());
    assertEquals(expected, result);
  }

  @Test
  void failedReplayDoesNotAffectTheNextReplay() {
    ReplayRunner replayRunner = new ReplayRunner();
    List<EngineCommand> invalidCommands = List.of(new PlaceOrder(1L, Side.ASK, 100L, 5L),
        new PlaceOrder(1L, Side.ASK, 101L, 2L));

    assertThrows(IllegalArgumentException.class, () -> replayRunner.replay(invalidCommands));

    PlaceOrder order = new PlaceOrder(1L, Side.BID, 99L, 3L);
    ReplayResult expected = new ReplayResult(List.of(new PlaceResult(1L, List.of(), 3L)),
        List.of(new OrderSnapshot(order, 3L)));
    assertEquals(expected, replayRunner.replay(List.of(order)));
  }
}
