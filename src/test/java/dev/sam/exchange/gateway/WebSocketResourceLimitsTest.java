package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;

import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
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

  @Test
  void pendingLimitIsPerSocketAndCompletionReturnsCapacityWithoutExecutingRejectedWork() throws Exception {
    var transport = new ControlledTransport();
    var limits = new WebSocketLimits(4, 2, 16384, 1048576, 60000, 5000);
    try (Fixture f = new Fixture(limits, transport);
        var first = new WebSocketTestClient(f.server.port());
        var second = new WebSocketTestClient(f.server.port())) {
      for (long id = 1; id <= 3; id++)
        first.socket.sendText(WebSocketOrderHandlerTest.cancel(id), true).get(3, TimeUnit.SECONDS);
      var rejected = first.response();
      assertEquals(WebSocketOrderHandlerTest.id(3), rejected.get("requestId").getAsString());
      assertEquals("RESOURCE_EXHAUSTED", rejected.getAsJsonObject("error").get("code").getAsString());
      assertFalse(rejected.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
      CommandRequest one = transport.next();
      CommandRequest two = transport.next();
      assertEquals(1, one.command().orderId());
      assertEquals(2, two.command().orderId());

      second.socket.sendText(WebSocketOrderHandlerTest.cancel(4), true).get(3, TimeUnit.SECONDS);
      CommandRequest four = transport.next();
      assertEquals(4, four.command().orderId(), "Rejected request 3 must never reach the transport");
      transport.reply(four);
      assertEquals(WebSocketOrderHandlerTest.id(4), second.response().get("requestId").getAsString());
      transport.reply(one);
      assertEquals(WebSocketOrderHandlerTest.id(1), first.response().get("requestId").getAsString());
      first.socket.sendText(WebSocketOrderHandlerTest.cancel(5), true).get(3, TimeUnit.SECONDS);
      CommandRequest five = transport.next();
      assertEquals(5, five.command().orderId());
      transport.reply(two);
      transport.reply(five);
      assertEquals(WebSocketOrderHandlerTest.id(2), first.response().get("requestId").getAsString());
      assertEquals(WebSocketOrderHandlerTest.id(5), first.response().get("requestId").getAsString());
      assertTrue(transport.sent.isEmpty());
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

  private static final class ControlledTransport extends AeronRequestClient {
    private final SbeRequestCodec codec = new SbeRequestCodec();
    final BlockingQueue<CommandRequest> sent = new LinkedBlockingQueue<>();
    final BlockingQueue<CommandResponse> replies = new LinkedBlockingQueue<>();

    ControlledTransport() {
      super(null, null, new ClientConfig(Duration.ofSeconds(2), 1));
    }

    @Override
    public long trySend(DirectBuffer buffer, int offset, int length) {
      sent.add(codec.decode(buffer, offset, length));
      return 1;
    }

    @Override
    public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
      int count = 0;
      CommandResponse response;
      while (count < fragmentLimit && (response = replies.poll()) != null) {
        onResponse.accept(response);
        count++;
      }
      return count;
    }

    CommandRequest next() throws InterruptedException {
      CommandRequest request = sent.poll(3, TimeUnit.SECONDS);
      assertNotNull(request, "Expected an admitted transport request");
      return request;
    }

    void reply(CommandRequest request) {
      replies.add(new CommandResponse(request.requestId(), new CancelResult(request.command().orderId(), false)));
    }
  }

  private static class Fixture implements AutoCloseable {
    final EngineGateway gateway;
    final WebSocketGateway server;

    Fixture(WebSocketLimits limits) throws Exception {
      this(limits, new ReplyingAeronClient(request -> new CancelResult(request.command().orderId(), false)));
    }

    Fixture(WebSocketLimits limits, AeronRequestClient transport) throws Exception {
      gateway = new EngineGateway(transport, 8, 4);
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
