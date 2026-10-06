package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonObject;

import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.OrderSnapshot;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.transport.AeronEngineAgent;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;
import dev.sam.exchange.transport.ResponsePublicationRegistry;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

@Timeout(40)
class WebSocketGatewayIntegrationTest {
  @Test
  void recordedOrdersAndRetryResultsSurviveEngineAndGatewayRestart(@TempDir Path directory) throws Exception {
    AtomicReference<JsonObject> original = new AtomicReference<>();
    var firstBook = withRecordedServer(directory, server -> {
      try (var client = new WebSocketTestClient(server.port())) {
        client.request(WebSocketGatewayTest.place(1, 101, "BID", 100, 10));
        original.set(client.request(WebSocketGatewayTest.place(2, 102, "ASK", 99, 4)));
        assertEquals("4", original.get().getAsJsonObject("place").getAsJsonArray("trades").get(0).getAsJsonObject()
            .get("quantityLots").getAsString());
      }
    });
    assertEquals(List.of(new OrderSnapshot(new PlaceOrder(101, Side.BID, 100, 10), 6)), firstBook);

    var recoveredBook = withRecordedServer(directory, server -> {
      try (var client = new WebSocketTestClient(server.port())) {
        assertEquals(original.get(), client.request(WebSocketGatewayTest.place(2, 102, "ASK", 99, 4)));
        var conflict = client.request(WebSocketGatewayTest.place(2, 999, "ASK", 99, 4));
        assertEquals("REQUEST_ID_CONFLICT", conflict.getAsJsonObject("reject").get("reason").getAsString());
        var next = client.request(WebSocketGatewayTest.place(3, 103, "ASK", 99, 10));
        assertEquals("6", next.getAsJsonObject("place").getAsJsonArray("trades").get(0).getAsJsonObject()
            .get("quantityLots").getAsString());
        assertEquals("4", next.getAsJsonObject("place").get("remainingLots").getAsString());
      }
    });
    assertEquals(List.of(new OrderSnapshot(new PlaceOrder(103, Side.ASK, 99, 10), 4)), recoveredBook);
    // Cached retries and conflicting reuse did not add another command to the recording.
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"),
        directory.resolve("driver").toString())) {
      List<CommandRequest> recorded = new ArrayList<>();
      ArchiveRequestLog.replay(runtime.archive(), recorded::add);
      assertEquals(List.of(101L, 102L, 103L),
          recorded.stream().map(request -> ((PlaceOrder) request.command()).orderId()).toList());
    }
  }

  private List<OrderSnapshot> withRecordedServer(Path directory, Exercise exercise) throws Exception {
    String driverDirectory = directory.resolve("driver").toString();
    OrderBook book = new OrderBook();
    var state = new RequestStateMachine(new MatchingEngine(book));
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"), driverDirectory);
        ArchiveRequestLog log = ArchiveRequestLog.open(runtime.archive(), state::process);
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDirectory));
        Subscription requests = aeron.addSubscription("aeron:ipc", 1);
        ResponsePublicationRegistry responsePublications = new ResponsePublicationRegistry(aeron);
        Publication outbound = aeron.addPublication("aeron:ipc", 1);
        Subscription inbound = aeron.addSubscription("aeron:ipc", 2);
        AgentRunner runner = new AgentRunner(new SleepingIdleStrategy(), error -> failure.compareAndSet(null, error),
            null, new AeronEngineAgent(requests, responsePublications, state, log, false));
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(outbound, inbound), 8, 4);
        WebSocketGateway server = new WebSocketGateway(gateway, 0)) {
      AgentRunner.startOnThread(runner);
      gateway.start();
      server.start();
      exercise.run(server);
    }
    assertNull(failure.get(), () -> "Engine agent failure: " + failure.get());
    return book.snapshot(); // Read only after the engine runner has stopped.
  }

  @FunctionalInterface
  private interface Exercise {
    void run(WebSocketGateway server) throws Exception;
  }
}
