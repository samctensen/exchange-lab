package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

class GatewayHttpHandlerTest {
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
