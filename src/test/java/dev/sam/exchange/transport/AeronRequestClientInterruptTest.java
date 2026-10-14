package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Publication;

class AeronRequestClientInterruptTest {
  private static final CommandRequest REQUEST = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
  private static final CancelResult RESULT = new CancelResult(7L, true);

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void interruptedCallerDoesNotOfferARequest() {
    ScriptedClient client = new ScriptedClient();
    Thread.currentThread().interrupt();

    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(REQUEST));

    assertInterrupted(failure, false);
    assertEquals(List.of(), client.offers);
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionDuringBackpressureStopsBeforeAnotherOffer() {
    ScriptedClient client = new ScriptedClient();
    client.onOffer = () -> {
      if (client.offers.size() == 1) {
        Thread.currentThread().interrupt();
        return Publication.BACK_PRESSURED;
      }
      return 64L;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(REQUEST));

    assertInterrupted(failure, false);
    assertEquals(List.of(REQUEST), client.offers);
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionAfterAcceptedOfferPreservesUnknownOutcome() {
    ScriptedClient client = new ScriptedClient();
    client.onOffer = () -> {
      Thread.currentThread().interrupt();
      return 64L;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(REQUEST));

    assertInterrupted(failure, true);
    assertEquals(List.of(REQUEST), client.offers);
    assertEquals(0, client.polls);
  }

  @Test
  void interruptionWhileAwaitingReplyStopsPollingAndRetries() {
    ScriptedClient client = new ScriptedClient();
    client.onPoll = handler -> {
      if (client.polls == 1) {
        Thread.currentThread().interrupt();
        return 0;
      }
      handler.accept(new CommandResponse(REQUEST.requestId(), RESULT));
      return 1;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(REQUEST));

    assertInterrupted(failure, true);
    assertEquals(List.of(REQUEST), client.offers);
    assertEquals(1, client.polls);
  }

  @Test
  void interruptionDuringResendPreservesTheEarlierAcceptedOutcome() {
    ScriptedClient client = new ScriptedClient();
    client.onOffer = () -> {
      if (client.offers.size() == 2) {
        Thread.currentThread().interrupt();
        return Publication.BACK_PRESSURED;
      }
      return 64L;
    };
    client.onPoll = handler -> {
      if (client.offers.size() == 1)
        return 0;
      handler.accept(new CommandResponse(REQUEST.requestId(), RESULT));
      return 1;
    };

    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(REQUEST));

    assertInterrupted(failure, true);
    assertEquals(List.of(REQUEST, REQUEST), client.offers);
  }

  private static void assertInterrupted(IllegalStateException failure, boolean offered) {
    assertTrue(Thread.currentThread().isInterrupted(), "The caller must retain its interrupt flag");
    assertTrue(failure.getMessage().contains("Interrupted"), failure.getMessage());
    if (offered) {
      assertTrue(failure.getMessage().contains(REQUEST.requestId().toString()), failure.getMessage());
      assertTrue(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    } else {
      assertFalse(failure.getMessage().contains("outcome unknown"), failure.getMessage());
    }
  }

  // Control only the network boundary; encoding, send loops, and response correlation remain real.
  private static final class ScriptedClient extends AeronRequestClient {
    private final List<CommandRequest> offers = new ArrayList<>();
    private LongSupplier onOffer = () -> 64L;
    private ToIntFunction<Consumer<CommandResponse>> onPoll = handler -> {
      handler.accept(new CommandResponse(REQUEST.requestId(), RESULT));
      return 1;
    };
    private int polls;

    private ScriptedClient() {
      super(null, null, new ClientConfig(Duration.ofMillis(20), 3));
    }

    @Override
    public long trySend(DirectBuffer buffer, int offset, int length) {
      offers.add(new SbeRequestCodec().decode(buffer, offset, length));
      return onOffer.getAsLong();
    }

    @Override
    public int pollResponses(Consumer<CommandResponse> handler, int fragmentLimit) {
      polls++;
      return onPoll.applyAsInt(handler);
    }
  }
}
