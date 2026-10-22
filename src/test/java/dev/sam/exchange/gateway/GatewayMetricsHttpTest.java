package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
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

  @Test
  void headDescribesTheGetRepresentationWithoutSendingItsBody() {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> false, () -> METRICS));
    try {
      channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, "/metrics"));
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(200, response.status().code());
        assertEquals("49", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("text/plain; version=0.0.4; charset=utf-8", response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
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

  @ParameterizedTest
  @ValueSource(strings = {"POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE", "CONNECT"})
  void unsupportedMethodsAdvertiseGetAndHeadWithoutRendering(String method) {
    assertRejectedWithoutRendering(
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), "/metrics"), 405);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD"})
  void rejectsRequestBodiesWithoutRendering(String method) {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), "/metrics",
        Unpooled.wrappedBuffer(new byte[]{1}));
    request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 1);
    assertRejectedWithoutRendering(request, 400);
  }

  @Test
  void rejectsMalformedHttpWithoutRendering() {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics");
    request.setDecoderResult(DecoderResult.failure(new IllegalArgumentException("Invalid framing")));
    assertRejectedWithoutRendering(request, 400);
  }

  @ParameterizedTest
  @ValueSource(strings = {"https://example.com", "null", "http://localhost:65536", "http://localhost/",
      "http://localhost@evil.test"})
  void rejectsForbiddenOriginsWithoutRendering(String origin) {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics");
    request.headers().set(HttpHeaderNames.ORIGIN, origin);
    assertRejectedWithoutRendering(request, 403);
  }

  @Test
  void rejectsMultipleOriginsWithoutRendering() {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/metrics");
    request.headers().add(HttpHeaderNames.ORIGIN, "http://localhost").add(HttpHeaderNames.ORIGIN, "http://localhost");
    assertRejectedWithoutRendering(request, 403);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/metrics/", "/metrics?query=1", "/metrics?", "/metrics#fragment", "/%6detrics"})
  void onlyTheExactMetricsPathIsRecognized(String path) {
    assertRejectedWithoutRendering(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path), 404);
  }

  private static void assertRejectedWithoutRendering(DefaultFullHttpRequest request, int status) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> {
      throw new AssertionError("Rejected requests must not inspect readiness");
    }, () -> {
      throw new AssertionError("Rejected requests must not render metrics");
    }));
    try {
      channel.writeInbound(request);
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(status, response.status().code());
        assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("close", response.headers().get(HttpHeaderNames.CONNECTION));
        assertEquals(0, response.content().readableBytes());
        if (status != 404)
          assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
        if (status == 405)
          assertEquals("GET, HEAD", response.headers().get(HttpHeaderNames.ALLOW));
      } finally {
        response.release();
      }
      assertNull(channel.readInbound(), "Rejected metrics requests must never reach the order handler");
      assertFalse(channel.isActive());
      assertEquals(0, request.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

}
