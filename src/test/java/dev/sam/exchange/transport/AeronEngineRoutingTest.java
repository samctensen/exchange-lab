package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.persistence.RequestLog;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

@Timeout(15)
class AeronEngineRoutingTest {
  @Test
  void registersAReplyRouteBeforeTheFirstRequestArrives() {
    try (Fixture fixture = new Fixture(); Client client = fixture.client()) {
      assertTrue(fixture.registry.find(client.route).isEmpty());
      assertEquals(1, fixture.agent.doWork(), "Discovering one image must count as work");
      fixture.awaitDriver(client.replies::isConnected, "connecting the response route");

      assertTrue(fixture.registry.find(client.route).orElseThrow().isConnected());
      assertEquals(0, fixture.agent.doWork(), "An unchanged connection must not count as new work");
      assertEquals(List.of(), fixture.log.accepted);
    }
  }

  @Test
  void loggedRequestsAndCachedRetriesKeepTheirOwnReplyRoutes() {
    try (Fixture fixture = new Fixture(); Client a = fixture.client(); Client b = fixture.client()) {
      fixture.log.recordImmediately = false;
      PlaceOrder bid = new PlaceOrder(1, Side.BID, 100, 10);
      CommandRequest first = new CommandRequest(new UUID(0, 1), bid);
      CommandRequest second = new CommandRequest(new UUID(0, 2), new PlaceOrder(2, Side.ASK, 99, 4));
      fixture.send(a, first);
      fixture.await(() -> fixture.log.accepted.size() == 1, "logging the first request");
      fixture.send(b, second);
      fixture.await(() -> fixture.log.accepted.size() == 2, "logging the second request");
      assertTrue(fixture.book.snapshot().isEmpty());

      fixture.log.recordedPosition = 128;
      fixture.await(() -> {
        a.poll();
        b.poll();
        return a.received.size() == 1 && b.received.size() == 1;
      }, "receiving the routed results");
      CommandResponse original = new CommandResponse(first.requestId(), new PlaceResult(1, List.of(), 10));
      CommandResponse fill = new CommandResponse(second.requestId(),
          new PlaceResult(2, List.of(new Trade(2, 1, 100, 4)), 0));
      assertEquals(List.of(original), a.received);
      assertEquals(List.of(fill), b.received);

      // The same UUID arrives on B's connection; its cached response must now be sent to B.
      fixture.send(b, first);
      fixture.await(() -> {
        a.poll();
        b.poll();
        return b.received.size() == 2;
      }, "receiving the cached response on the retrying connection");
      assertEquals(List.of(original), a.received);
      assertEquals(List.of(fill, original), b.received);
      assertEquals(List.of(first, second), fixture.log.accepted);
      assertEquals(List.of(new OrderSnapshot(bid, 6)), fixture.book.snapshot());
    }
  }

  @Test
  void registersANewImageEvenWhenAReplyWasAlreadyPending() {
    try (Fixture fixture = new Fixture(); Client a = fixture.client()) {
      fixture.send(a, new CommandRequest(new UUID(0, 1), new CancelOrder(1)));
      fixture.agent.doWork();
      assertEquals(1, fixture.log.accepted.size());
      assertTrue(fixture.registry.find(a.route).isEmpty(), "The driver has not processed the async registration");

      try (Client b = fixture.client()) {
        // Synchronous client creation advances the driver, but never calls the engine agent.
        fixture.agent.doWork();
        fixture.awaitDriver(b.replies::isConnected, "discovering B while A has a pending reply");
        assertTrue(fixture.registry.find(b.route).isPresent());
        assertEquals(1, fixture.log.accepted.size());
      }
    }
  }

  @Test
  void removesADepartedImageWhileItsReplyIsPendingWithoutResettingTheDeadline() {
    try (Fixture fixture = new Fixture(); Client client = fixture.client()) {
      fixture.await(client.replies::isConnected, "connecting the response route");
      Publication replies = fixture.registry.find(client.route).orElseThrow();
      client.replies.close();
      fixture.awaitDriver(() -> !replies.isConnected(), "disconnecting the reply subscriber");

      CommandRequest request = new CommandRequest(new UUID(0, 1), new CancelOrder(1));
      fixture.send(client, request);
      fixture.agent.doWork();
      assertEquals(List.of(request), fixture.log.accepted);
      fixture.clock.now = TimeUnit.SECONDS.toNanos(4);
      client.requests.close();
      fixture.awaitDriver(() -> fixture.requests.imageCount() == 0, "removing the request image");

      assertEquals(1, fixture.agent.doWork(), "Removal must happen even while the reply is pending");
      assertTrue(fixture.registry.find(client.route).isEmpty());
      assertTrue(replies.isClosed());
      fixture.clock.now = TimeUnit.SECONDS.toNanos(5);
      IllegalStateException failure = assertThrows(IllegalStateException.class, fixture.agent::doWork);
      assertTrue(failure.getMessage().contains("Timed out sending reply"));
      assertEquals(List.of(request), fixture.log.accepted);
    }
  }

  @Test
  void anUnreadyRegistrationUsesTheOriginalReplyDeadline() {
    try (Fixture fixture = new Fixture(); Client client = fixture.client()) {
      CommandRequest request = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
      fixture.send(client, request);
      fixture.agent.doWork();
      assertTrue(fixture.registry.find(client.route).isEmpty());

      // Do not advance the driver: lookup must stay empty and must not extend the reply deadline.
      fixture.clock.now = TimeUnit.SECONDS.toNanos(4);
      assertEquals(0, fixture.agent.doWork());
      fixture.clock.now = TimeUnit.SECONDS.toNanos(5);
      IllegalStateException failure = assertThrows(IllegalStateException.class, fixture.agent::doWork);
      assertTrue(failure.getMessage().contains("Timed out sending reply"));
      assertEquals(List.of(request), fixture.log.accepted);
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) request.command(), 10)), fixture.book.snapshot());
    }
  }

  @Test
  void reconnectGetsTheCachedResultThroughANewRoute() {
    try (Fixture fixture = new Fixture()) {
      CommandRequest request = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
      CommandResponse expected = new CommandResponse(request.requestId(), new PlaceResult(1, List.of(), 10));
      long oldRoute;
      Publication oldReplies;
      try (Client first = fixture.client()) {
        oldRoute = first.route;
        fixture.send(first, request);
        fixture.await(() -> {
          first.poll();
          return !first.received.isEmpty();
        }, "receiving the original reply");
        assertEquals(List.of(expected), first.received);
        oldReplies = fixture.registry.find(oldRoute).orElseThrow();
      }
      fixture.await(() -> fixture.registry.find(oldRoute).isEmpty(), "cleaning up the departed connection");
      assertTrue(oldReplies.isClosed());

      try (Client reconnected = fixture.client()) {
        assertFalse(oldRoute == reconnected.route);
        fixture.send(reconnected, request);
        fixture.await(() -> {
          reconnected.poll();
          return !reconnected.received.isEmpty();
        }, "receiving the cached result after reconnect");
        assertEquals(List.of(expected), reconnected.received);
        assertEquals(List.of(request), fixture.log.accepted);
        assertEquals(List.of(new OrderSnapshot((PlaceOrder) request.command(), 10)), fixture.book.snapshot());
      }
    }
  }

  private static final class Clock implements NanoClock {
    long now;
    public long nanoTime() {
      return now;
    }
  }

  private static final class Log implements RequestLog {
    final List<CommandRequest> accepted = new ArrayList<>();
    boolean recordImmediately = true;
    long recordedPosition;

    public long offer(CommandRequest request) {
      accepted.add(request);
      long position = accepted.size() * 64L;
      if (recordImmediately) {
        recordedPosition = position;
      }
      return position;
    }

    public boolean isRecorded(long position) {
      return recordedPosition >= position;
    }
  }

  private static final class Fixture implements AutoCloseable {
    final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().threadingMode(ThreadingMode.INVOKER)
        .publicationLingerTimeoutNs(0).dirDeleteOnShutdown(true));
    final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())
        .useConductorAgentInvoker(true).driverAgentInvoker(driver.sharedAgentInvoker()));
    final Subscription requests = aeron.addSubscription("aeron:ipc", 1);
    final ResponsePublicationRegistry registry = new ResponsePublicationRegistry(aeron);
    final OrderBook book = new OrderBook();
    final Log log = new Log();
    final Clock clock = new Clock();
    final AeronEngineAgent agent = new AeronEngineAgent(requests, registry,
        new RequestStateMachine(new MatchingEngine(book)), log, false, clock);

    Client client() {
      Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
      Publication commands = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(), 1);
      awaitDriver(() -> commands.isConnected() && requests.imageBySessionId(commands.sessionId()) != null,
          "connecting the request publication");
      return new Client(commands, replies, requests.imageBySessionId(commands.sessionId()).correlationId());
    }

    void send(Client client, CommandRequest request) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
      int length = new SbeRequestCodec().encode(request, buffer, 0);
      awaitDriver(() -> client.requests.offer(buffer, 0, length) >= 0, "sending the request");
    }

    void await(BooleanSupplier completed, String operation) {
      await(completed, operation, true);
    }

    void awaitDriver(BooleanSupplier completed, String operation) {
      await(completed, operation, false);
    }

    private void await(BooleanSupplier completed, String operation, boolean runAgent) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      do {
        driver.sharedAgentInvoker().invoke();
        aeron.conductorAgentInvoker().invoke();
        if (runAgent) {
          agent.doWork();
        }
        if (completed.getAsBoolean()) {
          return;
        }
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
          fail("Timed out or interrupted while " + operation);
        }
        idle.idle();
      } while (true);
    }

    public void close() {
      CloseHelper.closeAll(registry, requests, aeron, driver);
    }
  }

  private static final class Client implements AutoCloseable {
    final Publication requests;
    final Subscription replies;
    final long route;
    final List<CommandResponse> received = new ArrayList<>();
    final SbeResponseCodec codec = new SbeResponseCodec();
    final FragmentAssembler assembler = new FragmentAssembler(
        (buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length)));

    Client(Publication requests, Subscription replies, long route) {
      this.requests = requests;
      this.replies = replies;
      this.route = route;
    }

    void poll() {
      replies.poll(assembler, 10);
    }

    public void close() {
      CloseHelper.closeAll(requests, replies);
    }
  }
}
