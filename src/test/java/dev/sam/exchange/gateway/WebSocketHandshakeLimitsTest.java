package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.ServerHandshakeStateEvent;
import io.netty.util.ReferenceCountUtil;

class WebSocketHandshakeLimitsTest {
  @Test
  void stalledUpgradeClosesAtTheConfiguredHandshakeDeadline() {
    try (var fixture = new PipelineFixture()) {
      fixture.beginHandshake();
      assertNotNull(fixture.write.promise, "The actual HTTP upgrade response must reach the write boundary");
      assertFalse(fixture.write.promise.isDone());
      fixture.advance(249);
      assertTrue(fixture.channel.isActive(), "The handshake is still within its 250 ms budget");
      assertFalse(fixture.events.contains(ServerHandshakeStateEvent.HANDSHAKE_TIMEOUT));
      fixture.advance(1);
      assertFalse(fixture.channel.isActive(), "The stalled handshake must close at 250 ms");
      assertTrue(fixture.events.contains(ServerHandshakeStateEvent.HANDSHAKE_TIMEOUT));
    }
  }

  @Test
  void finishingTheUpgradeCancelsItsHandshakeTimeout() {
    try (var fixture = new PipelineFixture()) {
      fixture.beginHandshake();
      fixture.advance(249);
      fixture.write.complete();
      fixture.channel.runPendingTasks();
      assertTrue(fixture.events.stream().anyMatch(WebSocketServerProtocolHandler.HandshakeComplete.class::isInstance));
      fixture.advance(1);
      assertTrue(fixture.channel.isActive());
      assertFalse(fixture.events.contains(ServerHandshakeStateEvent.HANDSHAKE_TIMEOUT));
    }
  }

  private static final class PipelineFixture implements AutoCloseable {
    final EngineGateway gateway = new EngineGateway(new ReplyingAeronClient(request -> null), 1);
    final WebSocketGateway server = new WebSocketGateway(gateway, 0,
        new WebSocketLimits(1, 1, 16384, 1048576, 60000, 250));
    final HeldWrite write = new HeldWrite();
    final EmbeddedChannel channel = new EmbeddedChannel(write);
    final List<Object> events = new ArrayList<>();

    PipelineFixture() {
      channel.freezeTime();
      server.configurePipeline(channel.pipeline());
      channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object event) {
          events.add(event);
        }
      });
    }

    void beginHandshake() {
      String request = "GET /orders HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
          + "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
      channel.writeInbound(Unpooled.copiedBuffer(request, StandardCharsets.US_ASCII));
    }

    void advance(long millis) {
      channel.advanceTimeBy(millis, TimeUnit.MILLISECONDS);
      channel.runScheduledPendingTasks();
      channel.runPendingTasks();
    }

    @Override
    public void close() {
      write.release();
      channel.finishAndReleaseAll();
      server.close();
      gateway.close();
    }
  }

  private static final class HeldWrite extends ChannelOutboundHandlerAdapter {
    private Object message;
    private ChannelHandlerContext context;
    ChannelPromise promise;

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
      if (this.promise == null) {
        this.context = context;
        this.message = message;
        this.promise = promise;
      } else {
        context.write(message, promise);
      }
    }

    void complete() {
      Object response = message;
      message = null;
      context.writeAndFlush(response, promise);
    }

    void release() {
      if (message != null) {
        ReferenceCountUtil.release(message);
        message = null;
      }
    }
  }
}
