package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Publication;

@Timeout(5)
class AeronBenchmarkPipelineTest {
  @Test
  void refillsOneFreedSlotAndTimesOutOfOrderRepliesByUuid() {
    TestClock clock = new TestClock();
    ScriptedClient client = new ScriptedClient(100);
    client.onPoll = handler -> {
      switch (client.polls) {
        case 1 -> {
          assertEquals(2, client.accepted.size());
          clock.now = 20;
          handler.accept(response(client.accepted.get(1)));
          return 1;
        }
        case 2 -> {
          assertEquals(3, client.accepted.size(), "Refill the freed slot while the first request is still pending");
          clock.now = 30;
          handler.accept(response(client.accepted.get(2)));
          return 1;
        }
        case 3 -> {
          assertEquals(4, client.accepted.size());
          clock.now = 40;
          handler.accept(response(client.accepted.get(3)));
          clock.now = 50;
          handler.accept(response(client.accepted.getFirst()));
          return 2;
        }
        default -> throw new AssertionError("All requests should have completed");
      }
    };

    var measurement = AeronLatencyBenchmark.measure(client, 4, 2, clock, new BusySpinIdleStrategy());

    assertArrayEquals(new long[]{50, 20, 10, 10}, measurement.latencies());
    assertEquals(50, measurement.elapsedNanos());
    assertEquals(4, client.offers.size());
    assertEquals(4L, client.accepted.stream().map(CommandRequest::requestId).distinct().count());
  }

  @Test
  void pollsRepliesWhileAnOfferIsBackPressuredAndRetainsItsOriginalTimer() {
    TestClock clock = new TestClock();
    ScriptedClient client = new ScriptedClient(100);
    client.offerResults.addAll(List.of(64L, Publication.BACK_PRESSURED, 128L));
    client.onPoll = handler -> {
      if (client.polls == 1) {
        assertEquals(2, client.offers.size());
        assertEquals(1, client.accepted.size());
        clock.now = 10;
        handler.accept(response(client.accepted.getFirst()));
      } else {
        assertEquals(2, client.accepted.size());
        clock.now = 20;
        handler.accept(response(client.accepted.get(1)));
      }
      return 1;
    };

    var measurement = AeronLatencyBenchmark.measure(client, 2, 2, clock, new BusySpinIdleStrategy());

    assertArrayEquals(new long[]{10, 20}, measurement.latencies());
    assertEquals(3, client.offers.size());
    assertEquals(client.offers.get(1), client.offers.get(2), "Back pressure must retain the encoded request and UUID");
  }

  @Test
  void unrelatedAndDuplicateRepliesDoNotCompleteOtherSamples() {
    TestClock clock = new TestClock();
    ScriptedClient client = new ScriptedClient(100);
    client.onPoll = handler -> {
      clock.now += 10;
      switch (client.polls) {
        case 1 -> handler.accept(new CommandResponse(new UUID(0, 1), new CancelResult(1, false)));
        case 2, 3 -> handler.accept(response(client.accepted.getFirst()));
        case 4 -> handler.accept(response(client.accepted.get(1)));
        default -> throw new AssertionError("All requests should have completed");
      }
      return 1;
    };

    var measurement = AeronLatencyBenchmark.measure(client, 2, 1, clock, new BusySpinIdleStrategy());

    assertArrayEquals(new long[]{20, 20}, measurement.latencies());
    assertEquals(2, client.offers.size());
    assertEquals(4, client.polls);
  }

  @Test
  void otherCompletionsDoNotExtendAStalledRequestsReplyDeadline() {
    TestClock clock = new TestClock();
    ScriptedClient client = new ScriptedClient(10);
    client.onPoll = handler -> {
      clock.now += 6;
      handler.accept(response(client.accepted.getLast()));
      return 1;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> AeronLatencyBenchmark.measure(client, 6, 2, clock, new BusySpinIdleStrategy()));

    assertTrue(failure.getMessage().contains("No reply after 1 attempts"));
    assertTrue(failure.getMessage().contains(client.accepted.getFirst().requestId().toString()));
    assertEquals(3, client.offers.size());
    assertEquals(2, client.polls);
  }

  @Test
  void repeatedBackPressureDoesNotExtendTheOfferDeadline() {
    TestClock clock = new TestClock();
    ScriptedClient client = new ScriptedClient(10);
    client.offerResults.addAll(List.of(Publication.BACK_PRESSURED, Publication.BACK_PRESSURED,
        Publication.BACK_PRESSURED, Publication.BACK_PRESSURED));
    client.onPoll = handler -> {
      clock.now += 4;
      return 0;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> AeronLatencyBenchmark.measure(client, 2, 2, clock, new BusySpinIdleStrategy()));

    assertTrue(failure.getMessage().contains("Timed out sending"));
    assertEquals(0, client.accepted.size());
    assertEquals(4, client.offers.size());
    assertEquals(1L, client.offers.stream().map(CommandRequest::requestId).distinct().count());
  }

  @ParameterizedTest
  @ValueSource(longs = {Publication.CLOSED, Publication.MAX_POSITION_EXCEEDED})
  void terminalPublicationFailureAbortsImmediately(long offerResult) {
    ScriptedClient client = new ScriptedClient(100);
    client.offerResults.add(offerResult);

    assertThrows(IllegalStateException.class,
        () -> AeronLatencyBenchmark.measure(client, 2, 2, new TestClock(), new BusySpinIdleStrategy()));

    assertEquals(1, client.offers.size());
    assertEquals(0, client.polls);
  }

  @Test
  void throughputUsesTotalRunDurationRatherThanSummedOverlappingLatencies() {
    String report = AeronLatencyBenchmark.summarize(new long[]{1_000_000, 1_000_000, 1_000_000, 1_000_000}, 2_000_000);
    assertTrue(report.contains("Throughput: 2000.000 requests/s"), report);
  }

  private static CommandResponse response(CommandRequest request) {
    return new CommandResponse(request.requestId(), new CancelResult(1, false));
  }

  private static final class TestClock implements NanoClock {
    private long now;
    public long nanoTime() {
      return now;
    }
  }

  // Replace only network I/O; the benchmark still encodes requests, correlates replies, and measures time.
  private static final class ScriptedClient extends AeronRequestClient {
    private final List<CommandRequest> offers = new ArrayList<>();
    private final List<CommandRequest> accepted = new ArrayList<>();
    private final Deque<Long> offerResults = new ArrayDeque<>();
    private ToIntFunction<Consumer<CommandResponse>> onPoll = handler -> {
      throw new AssertionError("No response script configured");
    };
    private int polls;

    ScriptedClient(long timeoutNanos) {
      super(null, null, new ClientConfig(Duration.ofNanos(timeoutNanos), 1));
    }

    @Override
    public long trySend(DirectBuffer buffer, int offset, int length) {
      CommandRequest request = new SbeRequestCodec().decode(buffer, offset, length);
      offers.add(request);
      long result = offerResults.isEmpty() ? (accepted.size() + 1) * 64L : offerResults.removeFirst();
      if (result >= 0)
        accepted.add(request);
      return result;
    }

    @Override
    public int pollResponses(Consumer<CommandResponse> handler, int fragmentLimit) {
      assertTrue(++polls <= 20, "Benchmark failed to make progress");
      return onPoll.applyAsInt(handler);
    }
  }
}
