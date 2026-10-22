package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.engine.CancelResult;

@Timeout(15)
class WebSocketResourceLimitsTest {
  @Test
  void rejectsExcessTcpConnectionsAndReusesTheSlotAfterDisconnect() throws Exception {
    var limits = new WebSocketLimits(1, 32, 16384, 1048576, 60000, 5000);
    try (Fixture f = new Fixture(limits)) {
      try (var admitted = new WebSocketTestClient(f.server.port());
          var excess = new Socket("127.0.0.1", f.server.port())) {
        assertTrue(admitted.request(WebSocketOrderHandlerTest.cancel(1)).has("cancel"));
        excess.setSoTimeout(1000);
        assertEquals(-1, excess.getInputStream().read(), "The extra TCP connection must close before upgrade");
      }
      try (var replacement = connectWhenSlotReleased(f.server.port())) {
        assertTrue(replacement.request(WebSocketOrderHandlerTest.cancel(2)).has("cancel"));
      }
    }
  }

  private static WebSocketTestClient connectWhenSlotReleased(int port) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (true) {
      try {
        return new WebSocketTestClient(port);
      } catch (ExecutionException closedBeforeUpgrade) {
        if (System.nanoTime() - deadline >= 0)
          throw closedBeforeUpgrade;
      }
    }
  }

  private static class Fixture implements AutoCloseable {
    final EngineGateway gateway;
    final WebSocketGateway server;

    Fixture(WebSocketLimits limits) throws Exception {
      gateway = new EngineGateway(
          new ReplyingAeronClient(request -> new CancelResult(request.command().orderId(), false)), 8, 4);
      gateway.start();
      server = new WebSocketGateway(gateway, 0, limits);
      try {
        server.start();
      } catch (Exception | Error failure) {
        server.close();
        gateway.close();
        throw failure;
      }
    }

    @Override
    public void close() {
      server.close();
      gateway.close();
    }
  }
}
