package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

class GatewayMetricsHttpTest {
  private static final String METRICS = "# HELP scrape Café\n# TYPE scrape gauge\nscrape 1\n";

  @Test
  void enabledGetReturnsUtf8ExpositionEvenWhenGatewayIsUnavailable() {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> false, () -> METRICS));
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics");
    try {
      channel.writeInbound(request);
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(200, response.status().code());
        assertEquals(METRICS, response.content().toString(StandardCharsets.UTF_8));
        assertEquals("text/plain; version=0.0.4; charset=utf-8", response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertEquals("49", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
        assertEquals("close", response.headers().get(HttpHeaderNames.CONNECTION));
        assertFalse(channel.isActive());
      } finally {
        response.release();
      }
      assertNull(channel.readInbound(), "Metrics must never reach the order handler");
      assertEquals(0, request.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void metricsAreDisabledByDefault() {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> {
      throw new AssertionError("Unknown routes must not inspect readiness");
    }));
    try {
      channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics"));
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(404, response.status().code());
        assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals(0, response.content().readableBytes());
      } finally {
        response.release();
      }
      assertNull(channel.readInbound());
      assertFalse(channel.isActive());
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
