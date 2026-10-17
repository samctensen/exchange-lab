package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.ConnectException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
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

@Timeout(30)
class WebSocketHealthIntegrationTest {
  @Test
  @SuppressWarnings("try") // Stop the engine and listener early to exercise their independent lifecycles.
  void engineDisconnectChangesReadinessWhileProbesLeaveTheArchiveUntouched(@TempDir Path directory) throws Exception {
    String driverDirectory = directory.resolve("driver").toString();
    var state = new RequestStateMachine(new MatchingEngine(new OrderBook()));
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory.resolve("archive"), driverDirectory);
        ArchiveRequestLog log = ArchiveRequestLog.open(runtime.archive(), state::process);
        Aeron engineAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDirectory));
        Subscription commands = engineAeron.addSubscription("aeron:ipc", 1);
        ResponsePublicationRegistry responses = new ResponsePublicationRegistry(engineAeron);
        AgentRunner runner = new AgentRunner(new SleepingIdleStrategy(), error -> failure.compareAndSet(null, error),
            null, new AeronEngineAgent(commands, responses, state, log, false));
        Aeron clientAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDirectory));
        Subscription inbound = clientAeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication outbound = clientAeron
            .addPublication("aeron:ipc?response-correlation-id=" + inbound.registrationId(), 1);
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(outbound, inbound), 8, 4);
        WebSocketGateway server = new WebSocketGateway(gateway, 0)) {
      AgentRunner.startOnThread(runner);
      gateway.start();
      server.start();
      int port = server.port();
      awaitReadiness(port, "200 OK", "{\"status\":\"ready\"}");
      long beforeProbes = outbound.position();
      exerciseProbes(port);
      assertEquals(beforeProbes, outbound.position(), "Health must not publish a request to establish readiness");

      // A real order on the shared listener proves /orders still reaches the recorded engine.
      try (var websocket = new WebSocketTestClient(port)) {
        var result = websocket.request("""
            {"requestId":"00000000-0000-0000-0000-000000000001",
             "place":{"orderId":"101","side":"BID","priceTicks":"100","quantityLots":"10"}}
            """);
        assertEquals("10", result.getAsJsonObject("place").get("remainingLots").getAsString());
      }
      long afterOrder = outbound.position();
      assertTrue(afterOrder > beforeProbes);
      exerciseProbes(port);
      assertEquals(afterOrder, outbound.position(), "Repeated probes must leave the command stream unchanged");

      // Stop the actual engine route while leaving the client worker and HTTP listener alive.
      runner.close();
      commands.close();
      responses.close();
      log.close();
      awaitReadiness(port, "503 Service Unavailable", "{\"status\":\"not_ready\"}");
      var live = WebSocketHealthTest.exchange(port, "GET", "/health/live", "", "");
      assertEquals("HTTP/1.1 200 OK", live.status());
      assertEquals("{\"status\":\"live\"}", live.body());
      assertFalse(outbound.isClosed(), "Readiness loss must come from the disconnected engine route");
      assertFalse(inbound.isClosed());
      assertEquals(afterOrder, outbound.position(), "Disconnect detection must not submit a probe command");
      List<CommandRequest> recorded = new ArrayList<>();
      ArchiveRequestLog.replay(runtime.archive(), recorded::add);
      assertEquals(List.of(new CommandRequest(new UUID(0, 1), new PlaceOrder(101, Side.BID, 100, 10))), recorded,
          "The archive must contain the one explicit order and no probe-generated commands");
      assertNull(failure.get(), () -> "Engine failed: " + failure.get());

      server.close();
      assertThrows(ConnectException.class, () -> WebSocketHealthTest.exchange(port, "GET", "/health/live", "", ""));
    }
  }

  private static void exerciseProbes(int port) throws Exception {
    // Exceed the listener's 128 connection slots to catch probes that fail to release their slots.
    for (int i = 0; i < 140; i++) {
      String path = i % 2 == 0 ? "/health/live" : "/health/ready";
      String method = i % 4 < 2 ? "GET" : "HEAD";
      var response = WebSocketHealthTest.exchange(port, method, path, "", "");
      assertEquals("HTTP/1.1 200 OK", response.status());
      assertEquals("no-store", response.headers().get("Cache-Control"));
      assertEquals("close", response.headers().get("Connection"));
      if (method.equals("HEAD"))
        assertEquals("", response.body());
      else
        assertEquals(i % 2 == 0 ? "{\"status\":\"live\"}" : "{\"status\":\"ready\"}", response.body());
    }
  }

  private static void awaitReadiness(int port, String status, String body) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    var idle = new SleepingIdleStrategy();
    for (;;) {
      var response = WebSocketHealthTest.exchange(port, "GET", "/health/ready", "", "");
      if (response.status().equals("HTTP/1.1 " + status)) {
        assertEquals(body, response.body());
        return;
      }
      assertTrue(System.nanoTime() - deadline < 0, "Readiness never became " + status + ": " + response);
      idle.idle();
    }
  }
}
