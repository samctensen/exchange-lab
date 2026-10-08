package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;

import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
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
import io.aeron.ConcurrentPublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

@Timeout(15)
class AeronQueuedReplyTest {
  private static final long DEADLINE = TimeUnit.SECONDS.toNanos(5);
  private static final UUID FILLER_ID = new UUID(0, Long.MAX_VALUE);

  @Test
  void deliveryStatsSeparateOffersFromCapacityAndExpiryDrops() {
    try (Fixture fixture = new Fixture(new ReplyDeliveryConfig(2, 1024, Duration.ofSeconds(5)));
        Client full = fixture.client();
        Client expired = fixture.client();
        Client healthy = fixture.client()) {
      fixture.connect(full, expired, healthy);
      fixture.backPressure(full);
      fixture.backPressure(expired);
      CommandRequest first = request(1, new CancelOrder(1));
      fixture.submit(full, first);
      fixture.submit(full, request(2, new CancelOrder(2)));
      fixture.submit(full, request(3, new CancelOrder(3)));
      fixture.submit(expired, request(4, new CancelOrder(4)));
      fixture.clock.now = DEADLINE;
      fixture.agent.doWork();
      fixture.send(healthy, request(5, new CancelOrder(5)));
      fixture.receive(healthy, 1);
      assertEquals(1, fixture.stats.sentCount());
      assertEquals(3, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.CAPACITY));
      assertEquals(1, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.EXPIRED));
      assertEquals(0, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.UNAVAILABLE));
      assertTrue(fixture.stats.summarize().contains("offered=1, expired=1, capacity=3"));

      fixture.send(healthy, first);
      fixture.receive(healthy, 2);
      assertEquals(2, fixture.stats.sentCount(), "A cached retry is another delivery, not another execution");
      assertEquals(5, fixture.log.accepted.size());
    }
  }

  @ParameterizedTest
  @CsvSource({"2, 1024", "64, 66"})
  void configuredCapacityOrByteLimitClosesOnlyTheFullRoute(int capacity, long bytes) {
    try (Fixture fixture = new Fixture(new ReplyDeliveryConfig(capacity, bytes, Duration.ofSeconds(5)));
        Client blocked = fixture.client();
        Client healthy = fixture.client()) {
      fixture.connect(blocked, healthy);
      fixture.backPressure(blocked);
      fixture.submit(blocked, request(1, new CancelOrder(1)));
      fixture.submit(blocked, request(2, new CancelOrder(2)));
      assertTrue(fixture.registry.find(blocked.route).isPresent(), "Two 33-byte replies fit exactly");
      fixture.submit(blocked, request(3, new CancelOrder(3)));
      assertTrue(fixture.registry.find(blocked.route).isEmpty());
      fixture.send(healthy, request(4, new CancelOrder(4)));
      fixture.receive(healthy, 1);
      assertEquals(List.of(new CommandResponse(new UUID(0, 4), new CancelResult(4, false))), healthy.received);
      assertEquals(4, fixture.log.accepted.size());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {0, Long.MAX_VALUE - 5})
  void configuredReplyDeadlineWorksAcrossNanoClockWrap(long start) {
    try (Fixture fixture = new Fixture(new ReplyDeliveryConfig(64, 65536, Duration.ofNanos(10)));
        Client blocked = fixture.client()) {
      fixture.connect(blocked);
      fixture.backPressure(blocked);
      fixture.clock.now = start;
      fixture.submit(blocked, request(1, new CancelOrder(1)));
      fixture.clock.now = start + 9;
      assertEquals(0, fixture.agent.doWork());
      assertTrue(fixture.registry.find(blocked.route).isPresent());
      fixture.clock.now = start + 10;
      assertEquals(1, fixture.agent.doWork());
      assertTrue(fixture.registry.find(blocked.route).isEmpty());
      assertEquals(1, fixture.log.accepted.size());
    }
  }

  @Test
  void aClientArrivingAfterTheImageScanReceivesItsCachedReply() {
    try (Fixture fixture = new Fixture(); Client original = fixture.client()) {
      fixture.connect(original);
      CommandRequest command = request(1, new PlaceOrder(1, Side.BID, 100, 10));
      fixture.submit(original, command);
      Client[] joined = {null};
      try {
        fixture.registry.afterLookup = () -> {
          // The new image arrives after this duty cycle's scan, but before request polling.
          joined[0] = fixture.client();
          fixture.send(joined[0], command);
        };
        fixture.agent.doWork();
        fixture.receive(joined[0], 1);
        assertEquals(List.of(new CommandResponse(command.requestId(), new PlaceResult(1, List.of(), 10))),
            joined[0].received);
        assertEquals(List.of(command), fixture.log.accepted);
        assertEquals(List.of(new OrderSnapshot((PlaceOrder) command.command(), 10)), fixture.book.snapshot());
      } finally {
        CloseHelper.close(joined[0]);
      }
    }
  }

  @Test
  void sendsOneHeadPerRouteInOrderAndReencodesEachResponse() {
    try (Fixture fixture = new Fixture(); Client a = fixture.client(); Client b = fixture.client()) {
      fixture.connect(a, b);
      fixture.backPressure(a);
      fixture.backPressure(b);
      CommandRequest first = request(1, new CancelOrder(1));
      CommandRequest second = request(2, new CancelOrder(2));
      CommandRequest other = request(3, new PlaceOrder(3, Side.BID, 100, 7));
      fixture.submit(a, first);
      fixture.submit(a, second);
      fixture.submit(b, other);
      fixture.resume(a, b);

      assertEquals(2, fixture.agent.doWork());
      fixture.awaitDriver(() -> {
        a.poll();
        b.poll();
        return a.received.size() == 1 && b.received.size() == 1;
      }, "receiving one reply per route");
      assertEquals(List.of(new CommandResponse(first.requestId(), new CancelResult(1, false))), a.received);
      assertEquals(List.of(new CommandResponse(other.requestId(), new PlaceResult(3, List.of(), 7))), b.received);

      assertEquals(1, fixture.agent.doWork());
      fixture.awaitDriver(() -> {
        a.poll();
        return a.received.size() == 2;
      }, "receiving A's second reply");
      assertEquals(new CommandResponse(second.requestId(), new CancelResult(2, false)), a.received.getLast());
      assertEquals(0, fixture.agent.doWork());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void backpressureOnOneRouteDoesNotBlockAnother(boolean reverseRoutes) {
    try (Fixture fixture = new Fixture(); Client first = fixture.client(); Client second = fixture.client()) {
      Client a = reverseRoutes ? second : first;
      Client b = reverseRoutes ? first : second;
      fixture.connect(a, b);
      fixture.backPressure(a);
      PlaceOrder bid = new PlaceOrder(1, Side.BID, 100, 10);
      CommandRequest blocked = request(1, bid);
      CommandRequest healthy = request(2, new PlaceOrder(2, Side.ASK, 99, 4));
      fixture.clock.now = 10_000;
      fixture.submit(a, blocked);
      fixture.clock.now = 20_000;
      fixture.submit(b, healthy);
      fixture.receive(b, 1);
      assertEquals(
          List.of(new CommandResponse(healthy.requestId(), new PlaceResult(2, List.of(new Trade(2, 1, 100, 4)), 0))),
          b.received);
      assertEquals(List.of(new OrderSnapshot(bid, 6)), fixture.book.snapshot());
      assertTrue(fixture.timings.summarize().startsWith("Stage timing: 1/8 samples"));
      assertEquals(0, fixture.agent.doWork(), "A failed reply offer must not count as completed work");

      fixture.clock.now = 30_000;
      fixture.resume(a);
      fixture.receive(a, 1);
      CommandResponse original = new CommandResponse(blocked.requestId(), new PlaceResult(1, List.of(), 10));
      assertEquals(List.of(original), a.received, "Queued replies retain their original result as the book changes");
      String report = fixture.timings.summarize();
      assertTrue(report.startsWith("Stage timing: 2/8 samples"), report);
      assertTrue(report.contains("Reply offer: mean=10.000"), report);

      fixture.send(b, blocked);
      fixture.receive(b, 2);
      assertEquals(original, b.received.getLast());
      assertEquals(report, fixture.timings.summarize(), "Cached retries must not add timing samples");
      assertEquals(List.of(blocked, healthy), fixture.log.accepted);
      assertEquals(List.of(new OrderSnapshot(bid, 6)), fixture.book.snapshot());
    }
  }

  @Test
  void pendingRegistrationCannotExtendReplyDeadline() {
    try (Fixture fixture = new Fixture(); Client client = fixture.client()) {
      CommandRequest command = request(1, new PlaceOrder(1, Side.BID, 100, 10));
      fixture.send(client, command);
      fixture.agent.doWork();
      assertTrue(fixture.state.hasProcessed(command.requestId()));
      assertTrue(fixture.registry.find(client.route).isEmpty());

      // Leave the invoker driver stopped so registration cannot complete.
      fixture.clock.now = DEADLINE - 1;
      assertEquals(0, fixture.agent.doWork());
      fixture.clock.now = DEADLINE;
      assertEquals(1, fixture.agent.doWork());
      assertTrue(fixture.registry.find(client.route).isEmpty());
      assertEquals(0, fixture.agent.doWork(), "The live request image must not recreate a disabled reply route");
      assertEquals(List.of(command), fixture.log.accepted);
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) command.command(), 10)), fixture.book.snapshot());
    }
  }

  @Test
  void expiryDropsOnlyItsRoutesAndReconnectReturnsTheOriginalResult() {
    try (Fixture fixture = new Fixture();
        Client a = fixture.client();
        Client b = fixture.client();
        Client c = fixture.client()) {
      fixture.connect(a, b, c);
      fixture.backPressure(a);
      fixture.backPressure(b);
      Publication toA = fixture.registry.find(a.route).orElseThrow();
      Publication toB = fixture.registry.find(b.route).orElseThrow();
      CommandRequest first = request(1, new PlaceOrder(1, Side.ASK, 100, 5));
      CommandRequest second = request(2, new CancelOrder(2));
      fixture.submit(a, first);
      fixture.submit(b, second);
      fixture.clock.now = DEADLINE;
      assertEquals(2, fixture.agent.doWork());
      assertTrue(toA.isClosed());
      assertTrue(toB.isClosed());
      assertEquals(0, fixture.agent.doWork());
      assertTrue(fixture.registry.find(a.route).isEmpty());
      assertTrue(fixture.registry.find(b.route).isEmpty());
      assertTrue(fixture.registry.find(c.route).isPresent());

      CommandRequest trade = request(3, new PlaceOrder(3, Side.BID, 100, 2));
      fixture.send(c, trade);
      fixture.receive(c, 1);
      assertEquals(
          List.of(new CommandResponse(trade.requestId(), new PlaceResult(3, List.of(new Trade(3, 1, 100, 2)), 0))),
          c.received);
      try (Client reconnected = fixture.client()) {
        fixture.connect(reconnected);
        fixture.send(reconnected, first);
        fixture.receive(reconnected, 1);
        assertEquals(List.of(new CommandResponse(first.requestId(), new PlaceResult(1, List.of(), 5))),
            reconnected.received);
      }
      assertEquals(List.of(first, second, trade), fixture.log.accepted);
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) first.command(), 3)), fixture.book.snapshot());
    }
  }

  @Test
  void closedPublicationDropsOnlyItsRoute() {
    try (Fixture fixture = new Fixture(); Client a = fixture.client(); Client b = fixture.client()) {
      fixture.connect(a, b);
      CommandRequest first = request(1, new CancelOrder(1));
      fixture.submit(a, first);
      fixture.registry.closeOnLookup = a.route;
      assertEquals(1, fixture.agent.doWork());
      assertEquals(1, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.UNAVAILABLE));
      assertTrue(fixture.registry.find(a.route).isEmpty());
      assertEquals(0, fixture.agent.doWork());

      CommandRequest healthy = request(2, new CancelOrder(2));
      fixture.send(b, healthy);
      fixture.receive(b, 1);
      assertEquals(List.of(new CommandResponse(healthy.requestId(), new CancelResult(2, false))), b.received);
      assertEquals(List.of(first, healthy), fixture.log.accepted);
    }
  }

  @Test
  void disconnectCountsQueuedRepliesWithoutUndoingTheirOrders() {
    try (Fixture fixture = new Fixture(); Client client = fixture.client()) {
      fixture.connect(client);
      fixture.backPressure(client);
      PlaceOrder bid = new PlaceOrder(1, Side.BID, 100, 10);
      fixture.submit(client, request(1, bid));
      client.requests.close();
      fixture.awaitDriver(() -> fixture.requests.imageCount() == 0, "disconnecting the client");
      fixture.agent.doWork();
      assertEquals(1, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.DISCONNECTED));
      assertEquals(0, fixture.stats.sentCount());
      assertEquals(List.of(new OrderSnapshot(bid, 10)), fixture.book.snapshot());
      assertEquals(0, fixture.agent.doWork());
      assertEquals(1, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.DISCONNECTED));
    }
  }

  @ParameterizedTest(name = "commands={0}, initial asks={1}, quantity={2}")
  @CsvSource({"65, 0, 1", "21, 2100, 100"})
  void countOrByteOverflowDisablesOnlyThatRouteAndKeepsItsResults(int count, int asks, int quantity) {
    try (Fixture fixture = new Fixture(); Client a = fixture.client(); Client b = fixture.client()) {
      for (int id = 1; id <= asks; id++) {
        fixture.book.add(new PlaceOrder(id, Side.ASK, 100, 1));
      }
      fixture.connect(a, b);
      fixture.backPressure(a);
      CommandRequest last = null;
      for (int i = 1; i <= count; i++) {
        long id = 5000 + i;
        last = request(id, asks == 0 ? new CancelOrder(id) : new PlaceOrder(id, Side.BID, 100, quantity));
        fixture.submit(a, last);
        if (i < count) {
          assertTrue(fixture.registry.find(a.route).isPresent(), "The route must survive while replies fit");
        }
      }
      assertTrue(fixture.registry.find(a.route).isEmpty());
      assertTrue(fixture.book.snapshot().isEmpty(), "Overflow must not undo already-applied commands");
      assertEquals(count, fixture.log.accepted.size());
      assertEquals(0, fixture.agent.doWork());

      CommandRequest healthy = request(9000, new PlaceOrder(9000, Side.BID, 101, 2));
      fixture.send(b, healthy);
      fixture.receive(b, 1);
      assertEquals(List.of(new CommandResponse(healthy.requestId(), new PlaceResult(9000, List.of(), 2))), b.received);
      long lastOrder = 5000 + count;
      CommandResult expected = asks == 0
          ? new CancelResult(lastOrder, false)
          : new PlaceResult(lastOrder, LongStream.rangeClosed(asks - quantity + 1, asks)
              .mapToObj(id -> new Trade(lastOrder, id, 100, 1)).toList(), 0);
      try (Client reconnected = fixture.client()) {
        fixture.connect(reconnected);
        fixture.send(reconnected, last);
        fixture.receive(reconnected, 1);
        assertEquals(List.of(new CommandResponse(last.requestId(), expected)), reconnected.received);
      }
      assertEquals(count + 1, fixture.log.accepted.size(), "The reconnect retry must not append or execute again");
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) healthy.command(), 2)), fixture.book.snapshot());
    }
  }

  @Test
  void aRecordedRequestStillExecutesAfterItsClientDisconnects() {
    try (Fixture fixture = new Fixture(); Client a = fixture.client()) {
      fixture.connect(a);
      fixture.log.recordImmediately = false;
      CommandRequest command = request(1, new PlaceOrder(1, Side.BID, 100, 10));
      fixture.send(a, command);
      fixture.await(() -> fixture.log.accepted.size() == 1, "offering the command to the log");
      assertTrue(fixture.book.snapshot().isEmpty());
      a.requests.close();
      fixture.awaitDriver(() -> fixture.requests.imageCount() == 0, "disconnecting the request image");
      fixture.agent.doWork();
      fixture.log.recordedPosition = 64;
      fixture.await(() -> fixture.state.hasProcessed(command.requestId()),
          "applying the disconnected client's recorded command");
      assertEquals(1, fixture.stats.droppedCount(ReplyDeliveryStats.DropReason.UNAVAILABLE));
      assertEquals(List.of(new OrderSnapshot((PlaceOrder) command.command(), 10)), fixture.book.snapshot());

      try (Client reconnected = fixture.client()) {
        fixture.connect(reconnected);
        fixture.send(reconnected, command);
        fixture.receive(reconnected, 1);
        assertEquals(List.of(new CommandResponse(command.requestId(), new PlaceResult(1, List.of(), 10))),
            reconnected.received);
      }
      assertEquals(List.of(command), fixture.log.accepted);
    }
  }

  private static CommandRequest request(long id, EngineCommand command) {
    return new CommandRequest(new UUID(0, id), command);
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
      if (recordImmediately)
        recordedPosition = position;
      return position;
    }
    public boolean isRecorded(long position) {
      return recordedPosition >= position;
    }
  }

  private static final class Registry extends ResponsePublicationRegistry {
    long closeOnLookup = -1;
    Runnable afterLookup;
    Registry(Aeron aeron) {
      super(aeron);
    }
    @Override
    public Optional<ConcurrentPublication> find(long route) {
      Optional<ConcurrentPublication> publication = super.find(route);
      Runnable action = afterLookup;
      afterLookup = null;
      if (action != null)
        action.run();
      // Reproduce the conductor closing a real publication between lookup and offer.
      if (route == closeOnLookup)
        publication.ifPresent(Publication::close);
      return publication;
    }
  }

  private static final class Fixture implements AutoCloseable {
    final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().threadingMode(ThreadingMode.INVOKER)
        .ipcTermBufferLength(64 * 1024).publicationLingerTimeoutNs(0).dirDeleteOnShutdown(true));
    final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())
        .useConductorAgentInvoker(true).driverAgentInvoker(driver.sharedAgentInvoker()));
    final Subscription requests = aeron.addSubscription("aeron:ipc", 1);
    final Registry registry = new Registry(aeron);
    final Clock clock = new Clock();
    final EngineStageTimings timings = new EngineStageTimings(0, 8);
    final ReplyDeliveryStats stats = new ReplyDeliveryStats();
    final Log log = new Log();
    final OrderBook book = new OrderBook();
    final RequestStateMachine state = new RequestStateMachine(new MatchingEngine(book));
    final AeronEngineAgent agent;

    Fixture() {
      this(ReplyDeliveryConfig.defaults());
    }

    Fixture(ReplyDeliveryConfig config) {
      agent = new AeronEngineAgent(requests, registry, state, log, false, clock, timings,
          AeronEngineAgent.DEFAULT_LOG_WINDOW, config, stats);
    }

    Client client() {
      Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
      Publication commands = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(), 1);
      awaitDriver(() -> commands.isConnected() && requests.imageBySessionId(commands.sessionId()) != null,
          "connecting the request publication");
      return new Client(commands, replies, requests.imageBySessionId(commands.sessionId()).correlationId());
    }
    void connect(Client... clients) {
      agent.doWork();
      for (Client client : clients) {
        awaitDriver(
            () -> client.replies.isConnected()
                && registry.find(client.route).map(Publication::isConnected).orElse(false),
            "connecting the response publication");
      }
    }
    void send(Client client, CommandRequest command) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
      int length = new SbeRequestCodec().encode(command, buffer, 0);
      awaitDriver(() -> client.requests.offer(buffer, 0, length) >= 0, "sending the command");
    }
    void submit(Client client, CommandRequest command) {
      send(client, command);
      await(() -> state.hasProcessed(command.requestId()), "applying the command");
    }
    void receive(Client client, int count) {
      await(() -> {
        client.poll();
        return client.received.size() == count;
      }, "receiving replies");
    }
    void backPressure(Client client) {
      ConcurrentPublication publication = registry.find(client.route).orElseThrow();
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
      int length = new SbeResponseCodec().encode(new CommandResponse(FILLER_ID, new CancelResult(99, false)), buffer,
          0);
      awaitDriver(() -> publication.offer(buffer, 0, length) == Publication.BACK_PRESSURED,
          "filling the reply publication");
    }
    void resume(Client... clients) {
      for (Client client : clients) {
        Publication publication = registry.find(client.route).orElseThrow();
        awaitDriver(() -> {
          client.poll();
          return publication.availableWindow() >= 8192;
        }, "freeing reply publication capacity");
      }
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
        if (runAgent)
          agent.doWork();
        if (completed.getAsBoolean())
          return;
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
    final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
      CommandResponse response = codec.decode(buffer, offset, length);
      if (!response.requestId().equals(FILLER_ID))
        received.add(response);
    });
    Client(Publication requests, Subscription replies, long route) {
      this.requests = requests;
      this.replies = replies;
      this.route = route;
    }
    void poll() {
      replies.poll(assembler, 100);
    }
    public void close() {
      CloseHelper.closeAll(requests, replies);
    }
  }
}
