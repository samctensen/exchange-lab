package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Publication;

@Timeout(5)
class BenchmarkInterruptTest {
  @AfterEach
  void clearInterruptForTheNextTest() {
    Thread.interrupted();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 4})
  void preInterruptedMeasurementDoesNotOfferOrPoll(int count) {
    BoundaryClient client = new BoundaryClient();
    Thread.currentThread().interrupt();

    IllegalStateException failure = interruptedMeasurement(client, count, 4, new StepClock());

    assertFalse(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    assertEquals(0, client.offers.size());
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionWhilePreparingARequestPreventsItsOffer() {
    BoundaryClient client = new BoundaryClient();
    NanoClock clock = new NanoClock() {
      private long ticks;
      public long nanoTime() {
        if (++ticks == 2)
          Thread.currentThread().interrupt();
        return ticks;
      }
    };

    interruptedMeasurement(client, 4, 4, clock);

    assertEquals(0, client.offers.size());
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionUnderOfferPressureStopsWithoutRetryingOrPolling() {
    BoundaryClient client = new BoundaryClient();
    client.onOffer = request -> {
      Thread.currentThread().interrupt();
      return Publication.BACK_PRESSURED;
    };

    IllegalStateException failure = interruptedMeasurement(client, 4, 4, new StepClock());

    assertFalse(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    assertEquals(1, client.offers.size());
    assertEquals(0, client.accepted.size());
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionWhileWaitingForAReplyStopsPollingAndReportsTheUnknownOutcome() {
    BoundaryClient client = new BoundaryClient();
    client.onPoll = handler -> {
      Thread.currentThread().interrupt();
      return 0;
    };

    IllegalStateException failure = interruptedMeasurement(client, 4, 1, new StepClock());

    assertUnknownOutcome(failure, 1, client.accepted.getFirst());
    assertEquals(1, client.offers.size());
    assertEquals(1, client.polls);
  }

  @Test
  void interruptionInsideTheOfferBatchStopsBeforeAnotherOfferOrPoll() {
    BoundaryClient client = new BoundaryClient();
    client.onOffer = request -> {
      if (client.offers.size() == 3)
        Thread.currentThread().interrupt();
      return 64;
    };

    IllegalStateException failure = interruptedMeasurement(client, 12, 12, new StepClock());

    assertUnknownOutcome(failure, 3, client.accepted.getFirst());
    assertEquals(3, client.offers.size());
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionAfterOneReplyDoesNotRefillTheFreedWindowSlot() {
    BoundaryClient client = new BoundaryClient();
    client.onPoll = handler -> {
      handler.accept(response(client.accepted.getFirst()));
      Thread.currentThread().interrupt();
      return 1;
    };

    IllegalStateException failure = interruptedMeasurement(client, 8, 4, new StepClock());

    assertUnknownOutcome(failure, 3, client.accepted.get(1));
    assertEquals(4, client.offers.size());
    assertEquals(1, client.polls);
  }

  @Test
  void interruptionAtTheFinalReplyDoesNotReturnPerformanceMeasurements() {
    BoundaryClient client = new BoundaryClient();
    client.onPoll = handler -> {
      handler.accept(response(client.accepted.getFirst()));
      Thread.currentThread().interrupt();
      return 1;
    };

    IllegalStateException failure = interruptedMeasurement(client, 1, 1, new StepClock());

    assertFalse(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    assertEquals(1, client.offers.size());
    assertEquals(1, client.polls);
  }

  @Test
  void interruptionWithManyOutstandingRequestsKeepsTheDiagnosticBounded() {
    BoundaryClient client = new BoundaryClient();
    client.onOffer = request -> {
      if (client.offers.size() == 4096)
        Thread.currentThread().interrupt();
      return 64;
    };

    IllegalStateException failure = interruptedMeasurement(client, 4096, 4096, new StepClock());

    assertUnknownOutcome(failure, 4096, client.accepted.getFirst());
    assertTrue(failure.getMessage().length() < 300, "Report a representative UUID rather than the entire batch");
    assertEquals(4096, client.offers.size());
    assertEquals(0, client.polls);
  }

  private static IllegalStateException interruptedMeasurement(BoundaryClient client, int count, int window,
      NanoClock clock) {
    IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> AeronLatencyBenchmark.measure(client, count, window, clock, new BusySpinIdleStrategy()));
    assertTrue(Thread.currentThread().isInterrupted(), "The caller must retain its cancellation signal");
    assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
    return failure;
  }

  private static void assertUnknownOutcome(IllegalStateException failure, int outstanding, CommandRequest request) {
    assertTrue(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    assertTrue(failure.getMessage().contains("outstanding requests: " + outstanding), failure.getMessage());
    assertTrue(failure.getMessage().contains(request.requestId().toString()), failure.getMessage());
  }

  private static CommandResponse response(CommandRequest request) {
    return new CommandResponse(request.requestId(), new CancelResult(1, false));
  }

  private static final class StepClock implements NanoClock {
    private long ticks;
    public long nanoTime() {
      return ticks++;
    }
  }

  // Fake only publication/subscription I/O; measure still encodes, correlates, and times each request.
  private static final class BoundaryClient extends AeronRequestClient {
    private final List<CommandRequest> offers = new ArrayList<>();
    private final List<CommandRequest> accepted = new ArrayList<>();
    private final Deque<CommandRequest> awaitingReply = new ArrayDeque<>();
    private ToLongFunction<CommandRequest> onOffer = request -> 64;
    private ToIntFunction<Consumer<CommandResponse>> onPoll;
    private int polls;

    BoundaryClient() {
      super(null, null, new ClientConfig(Duration.ofNanos(100), 1));
    }

    @Override
    public long trySend(DirectBuffer buffer, int offset, int length) {
      CommandRequest request = new SbeRequestCodec().decode(buffer, offset, length);
      offers.add(request);
      long result = onOffer.applyAsLong(request);
      if (result >= 0) {
        accepted.add(request);
        awaitingReply.add(request);
      }
      return result;
    }

    @Override
    public int pollResponses(Consumer<CommandResponse> handler, int fragmentLimit) {
      polls++;
      if (onPoll != null)
        return onPoll.applyAsInt(handler);
      int fragments = 0;
      while (fragments < fragmentLimit && !awaitingReply.isEmpty()) {
        handler.accept(response(awaitingReply.removeFirst()));
        fragments++;
      }
      return fragments;
    }
  }
}
