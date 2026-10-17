package dev.sam.exchange.gateway;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/** Serves local probes and applies the same browser Origin policy to probes and order upgrades. */
final class GatewayHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private final BooleanSupplier ready;

  GatewayHttpHandler(BooleanSupplier ready) {
    this.ready = Objects.requireNonNull(ready);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
    boolean live = request.uri().equals("/health/live");
    boolean readiness = request.uri().equals("/health/ready");
    boolean health = live || readiness;
    HttpResponseStatus failure = null;
    if (!request.decoderResult().isSuccess())
      failure = HttpResponseStatus.BAD_REQUEST;
    else if (!health && !request.uri().equals("/orders"))
      failure = HttpResponseStatus.NOT_FOUND;
    else if (!request.method().equals(HttpMethod.GET))
      failure = HttpResponseStatus.METHOD_NOT_ALLOWED;
    else if (request.headers().getAll(HttpHeaderNames.ORIGIN).size() > 1
        || !allowedOrigin(request.headers().get(HttpHeaderNames.ORIGIN)))
      failure = HttpResponseStatus.FORBIDDEN;
    if (failure != null) {
      var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, failure);
      response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0).set(HttpHeaderNames.CONNECTION,
          HttpHeaderValues.CLOSE);
      if (failure.equals(HttpResponseStatus.METHOD_NOT_ALLOWED))
        response.headers().set(HttpHeaderNames.ALLOW, HttpMethod.GET.name());
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } else if (health) {
      boolean available = live || ready.getAsBoolean();
      String status = live ? "live" : available ? "ready" : "not_ready";
      byte[] body = ("{\"status\":\"" + status + "\"}").getBytes(StandardCharsets.UTF_8);
      var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
          available ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE, Unpooled.wrappedBuffer(body));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8")
          .setInt(HttpHeaderNames.CONTENT_LENGTH, body.length).set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } else {
      ctx.fireChannelRead(request.retain());
    }
  }

  private static boolean allowedOrigin(String origin) {
    if (origin == null)
      return true; // Native clients do not send browser Origin headers. This is not authentication.
    try {
      URI uri = URI.create(origin);
      return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
          && ("localhost".equals(uri.getHost()) || "127.0.0.1".equals(uri.getHost())) && uri.getRawUserInfo() == null
          && uri.getPort() <= 65535 && !uri.getRawAuthority().endsWith(":") && uri.getRawQuery() == null
          && uri.getRawFragment() == null && (uri.getRawPath() == null || uri.getRawPath().isEmpty());
    } catch (IllegalArgumentException invalid) {
      return false;
    }
  }
}
