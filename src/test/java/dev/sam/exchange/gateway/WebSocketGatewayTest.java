package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.transport.RequestStateMachine;

@Timeout(15)
class WebSocketGatewayTest {
  @Test
  void placesMatchesAndCancelsOverARealWebSocket() throws Exception {
    try (Fixture f = new Fixture(); var client = new WebSocketTestClient(f.server.port())) {
      var bid = client.request(place(1, 101, "BID", 100, 10));
      assertEquals("10", bid.getAsJsonObject("place").get("remainingLots").getAsString());
      var ask = client.request(place(2, 102, "ASK", 99, 4));
      assertEquals(WebSocketOrderHandlerTest.id(2), ask.get("requestId").getAsString());
      var trade = ask.getAsJsonObject("place").getAsJsonArray("trades").get(0).getAsJsonObject();
      assertEquals("101", trade.get("restingOrderId").getAsString());
      assertEquals("100", trade.get("priceTicks").getAsString());
      assertEquals("4", trade.get("quantityLots").getAsString());
      var cancelled = client
          .request("{\"requestId\":\"" + WebSocketOrderHandlerTest.id(3) + "\",\"cancel\":{\"orderId\":\"101\"}}");
      assertTrue(cancelled.getAsJsonObject("cancel").get("cancelled").getAsBoolean());
    }
  }

  @Test
  void reconnectRetryReturnsTheOriginalResultWithoutMatchingTwice() throws Exception {
    try (Fixture f = new Fixture()) {
      com.google.gson.JsonObject original;
      try (var client = new WebSocketTestClient(f.server.port())) {
        client.request(place(1, 101, "BID", 100, 10));
        original = client.request(place(2, 102, "ASK", 99, 4));
      }
      try (var reconnect = new WebSocketTestClient(f.server.port())) {
        assertEquals(original, reconnect.request(place(2, 102, "ASK", 99, 4)));
        var result = reconnect.request(place(3, 103, "ASK", 99, 10));
        assertEquals("6", result.getAsJsonObject("place").getAsJsonArray("trades").get(0).getAsJsonObject()
            .get("quantityLots").getAsString());
        assertEquals("4", result.getAsJsonObject("place").get("remainingLots").getAsString());
      }
    }
  }

  @Test
  void assemblesFragmentedTextAndRecoversAfterMalformedInput() throws Exception {
    try (Fixture f = new Fixture(); var client = new WebSocketTestClient(f.server.port())) {
      assertEquals("INVALID_ARGUMENT", client.request("{}").getAsJsonObject("error").get("code").getAsString());
      String request = WebSocketOrderHandlerTest.cancel(1);
      client.socket.sendText(request.substring(0, 20), false).get(3, TimeUnit.SECONDS);
      client.socket.sendText(request.substring(20), true).get(3, TimeUnit.SECONDS);
      assertTrue(client.response().has("cancel"));
    }
  }

  @Test
  void rejectsBinaryAndOversizedMessages() throws Exception {
    try (Fixture f = new Fixture()) {
      try (var client = new WebSocketTestClient(f.server.port())) {
        client.socket.sendBinary(ByteBuffer.wrap(new byte[]{1, 2}), true).get(3, TimeUnit.SECONDS);
        assertEquals(1003, client.closed.get(3, TimeUnit.SECONDS));
      }
      try (var client = new WebSocketTestClient(f.server.port())) {
        client.socket.sendText("x".repeat(9000), false).get(3, TimeUnit.SECONDS);
        client.socket.sendText("x".repeat(9000), true).get(3, TimeUnit.SECONDS);
        assertEquals(1009, client.closed.get(3, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void rejectsUnexpectedPathsAndCrossSiteBrowserOrigins() throws Exception {
    try (Fixture f = new Fixture(); HttpClient client = HttpClient.newHttpClient()) {
      var wrongPath = client.newWebSocketBuilder()
          .buildAsync(URI.create("ws://127.0.0.1:" + f.server.port() + "/wrong"), new WebSocket.Listener() {
          });
      assertThrows(ExecutionException.class, () -> wrongPath.get(3, TimeUnit.SECONDS));
      var crossSite = client.newWebSocketBuilder().header("Origin", "https://example.com")
          .buildAsync(URI.create("ws://127.0.0.1:" + f.server.port() + "/orders"), new WebSocket.Listener() {
          });
      assertThrows(ExecutionException.class, () -> crossSite.get(3, TimeUnit.SECONDS));
      WebSocket local = client.newWebSocketBuilder().header("Origin", "http://localhost:3000")
          .buildAsync(URI.create("ws://127.0.0.1:" + f.server.port() + "/orders"), new WebSocket.Listener() {
          }).get(3, TimeUnit.SECONDS);
      local.abort();
    }
  }

  @Test
  void closingTheListenerClosesClientsButLeavesBorrowedGatewayUsable() throws Exception {
    try (Fixture f = new Fixture(); var client = new WebSocketTestClient(f.server.port())) {
      f.server.close();
      assertEquals(1001, client.closed.get(3, TimeUnit.SECONDS));
      assertNotNull(f.gateway.submit(new dev.sam.exchange.transport.CommandRequest(java.util.UUID.randomUUID(),
          new dev.sam.exchange.engine.CancelOrder(1))).get(3, TimeUnit.SECONDS));
      f.server.close();
      assertThrows(IllegalStateException.class, f.server::start);
    }
  }

  @Test
  void failedBindCanBeClosedWithoutDisruptingTheExistingListener() throws Exception {
    try (Fixture f = new Fixture(); WebSocketGateway conflicting = new WebSocketGateway(f.gateway, f.server.port())) {
      assertThrows(java.io.IOException.class, conflicting::start);
      try (var client = new WebSocketTestClient(f.server.port())) {
        assertTrue(client.request(WebSocketOrderHandlerTest.cancel(1)).has("cancel"));
      }
    }
  }

  static String place(long request, long order, String side, long price, long lots) {
    return "{\"requestId\":\"" + WebSocketOrderHandlerTest.id(request) + "\",\"place\":{\"orderId\":\"" + order
        + "\",\"side\":\"" + side + "\",\"priceTicks\":\"" + price + "\",\"quantityLots\":\"" + lots + "\"}}";
  }

  private static class Fixture implements AutoCloseable {
    final EngineGateway gateway;
    final WebSocketGateway server;
    Fixture() throws Exception {
      var processor = new RequestStateMachine(new MatchingEngine(new OrderBook()));
      gateway = new EngineGateway(new ReplyingAeronClient(request -> processor.process(request).result()), 8, 4);
      gateway.start();
      server = new WebSocketGateway(gateway, 0);
      try {
        server.start();
      } catch (Exception | Error e) {
        server.close();
        gateway.close();
        throw e;
      }
    }
    @Override
    public void close() {
      server.close();
      gateway.close();
    }
  }
}
