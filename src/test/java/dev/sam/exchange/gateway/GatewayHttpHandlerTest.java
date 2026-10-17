package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

class GatewayHttpHandlerTest {
  @ParameterizedTest
  @CsvSource({"/health/live, GET, false, 200", "/health/ready, GET, true, 200", "/health/ready, GET, false, 503",
      "/health/live, HEAD, false, 200", "/health/ready, HEAD, true, 200", "/health/ready, HEAD, false, 503"})
  void healthResponsesMustNotBeCached(String path, String method, boolean ready, int status) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> ready));
    try {
      channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), path));
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(status, response.status().code());
        assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
      } finally {
        response.release();
      }
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @ParameterizedTest
  @CsvSource({"/health/live, GET", "/health/live, HEAD", "/health/ready, GET", "/health/ready, HEAD"})
  void rejectsProbeBodiesWithoutObservingReadiness(String path, String method) {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), path,
        Unpooled.wrappedBuffer(new byte[]{1, 2}));
    request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 2);
    assertRejectedWithoutReadiness(request, 400);
  }

  @Test
  void malformedProbeIsRejectedWithoutObservingReadiness() {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health/ready");
    request.setDecoderResult(DecoderResult.failure(new IllegalArgumentException("Invalid HTTP framing")));
    assertRejectedWithoutReadiness(request, 400);
  }

  @ParameterizedTest
  @CsvSource({"/health/live, https://example.com", "/health/ready, null", "/health/ready, http://localhost:65536"})
  void rejectsUnrelatedOrMalformedProbeOrigins(String path, String origin) {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
    request.headers().set(HttpHeaderNames.ORIGIN, origin);
    assertRejectedWithoutReadiness(request, 403);
  }

  @Test
  void rejectsMultipleProbeOrigins() {
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health/ready");
    request.headers().add(HttpHeaderNames.ORIGIN, "http://localhost").add(HttpHeaderNames.ORIGIN, "http://localhost");
    assertRejectedWithoutReadiness(request, 403);
  }

  @ParameterizedTest
  @CsvSource({"/health/live/", "/health/ready?query=1", "/health/live?", "/health/ready/", "/health/unknown"})
  void onlyExactProbePathsAreRecognized(String path) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> {
      throw new AssertionError("Unknown routes must not inspect readiness");
    }));
    try {
      channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path));
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(404, response.status().code());
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

  private static void assertRejectedWithoutReadiness(DefaultFullHttpRequest request, int status) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> {
      throw new AssertionError("Rejected probes must not inspect readiness");
    }));
    try {
      channel.writeInbound(request);
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(status, response.status().code());
        assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
        assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals(0, response.content().readableBytes());
      } finally {
        response.release();
      }
      assertNull(channel.readInbound(), "A rejected probe must not reach the order handler");
      assertFalse(channel.isActive());
      assertEquals(0, request.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @ParameterizedTest
  @CsvSource({"/health/live, POST", "/health/live, PUT", "/health/live, DELETE", "/health/live, PATCH",
      "/health/live, OPTIONS", "/health/live, TRACE", "/health/ready, POST", "/health/ready, PUT",
      "/health/ready, DELETE", "/health/ready, PATCH", "/health/ready, OPTIONS", "/health/ready, CONNECT"})
  void unsupportedMethodsReturnAllowWithoutObservingReadinessOrForwarding(String path, String method) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> {
      throw new AssertionError("Rejected methods must not inspect gateway readiness");
    }));
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), path);
    try {
      channel.writeInbound(request);
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(405, response.status().code());
        assertEquals("GET, HEAD", response.headers().get(HttpHeaderNames.ALLOW));
        assertEquals("no-store", response.headers().get(HttpHeaderNames.CACHE_CONTROL));
        assertEquals("0", response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals(0, response.content().readableBytes());
        assertFalse(channel.isActive());
      } finally {
        response.release();
      }
      assertNull(channel.readInbound(), "A rejected probe must not reach the order handler");
      assertEquals(0, request.refCnt());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @ParameterizedTest
  @CsvSource({"/health/live, false, 200, 17", "/health/ready, true, 200, 18", "/health/ready, false, 503, 22"})
  void headDescribesGetRepresentationWithoutSendingBody(String path, boolean ready, int status, int length) {
    var channel = new EmbeddedChannel(new GatewayHttpHandler(() -> ready));
    var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, path);
    try {
      channel.writeInbound(request);
      FullHttpResponse response = channel.readOutbound();
      try {
        assertEquals(status, response.status().code());
        assertEquals(Integer.toString(length), response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("application/json; charset=utf-8", response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertEquals(0, response.content().readableBytes());
        assertFalse(channel.isActive(), "A probe should free its connection slot");
      } finally {
        response.release();
      }
      assertEquals(0, request.refCnt(), "The HTTP handler owns and releases handled requests");
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
