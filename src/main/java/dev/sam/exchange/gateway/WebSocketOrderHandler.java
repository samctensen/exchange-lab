package dev.sam.exchange.gateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.transport.CommandRequest;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleStateEvent;

/** One instance per socket. All mutable state belongs to that socket's event loop. */
final class WebSocketOrderHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
  private static final int MAX_RESPONSE_BYTES = 1024 * 1024;
  private final Function<CommandRequest, CompletableFuture<CommandResult>> submit;
  private final int maxPending;
  private final WebSocketCommandCodec codec = new WebSocketCommandCodec();
  private int pending;

  WebSocketOrderHandler(Function<CommandRequest, CompletableFuture<CommandResult>> submit, int maxPending) {
    this.submit = submit;
    this.maxPending = maxPending;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
    if (!ctx.channel().isActive())
      return;
    if (!ctx.channel().isWritable()) {
      ctx.close();
      return;
    }
    if (!(frame instanceof TextWebSocketFrame text)) {
      ctx.writeAndFlush(new CloseWebSocketFrame(1003, "JSON text messages required"))
          .addListener(ChannelFutureListener.CLOSE);
      return;
    }
    final CommandRequest request;
    try {
      request = codec.decode(text.text());
    } catch (IllegalArgumentException e) {
      write(ctx, codec.error(null, "INVALID_ARGUMENT", "Invalid order message; see the WebSocket contract", false));
      return;
    }
    if (pending >= maxPending) {
      write(ctx, codec.error(request.requestId(), "RESOURCE_EXHAUSTED",
          "Connection limit reached; request not admitted", false));
      return;
    }
    pending++;
    try {
      submit.apply(request).whenComplete((result, failure) -> {
        // EngineGateway completes futures on its Aeron worker. Never encode JSON or write sockets there.
        try {
          ctx.executor().execute(() -> complete(ctx, request.requestId(), result, failure));
        } catch (RejectedExecutionException stopped) {
          // The socket event loop was shut down. The accepted command's outcome is unchanged.
        }
      });
    } catch (RuntimeException failure) {
      complete(ctx, request.requestId(), null, failure);
    }
  }

  private void complete(ChannelHandlerContext ctx, UUID requestId, CommandResult result, Throwable failure) {
    if (!ctx.channel().isActive()) {
      pending--;
      return;
    }
    String response;
    if (failure != null) {
      while (failure instanceof CompletionException && failure.getCause() != null)
        failure = failure.getCause();
      boolean overload = failure instanceof GatewayOverloadedException;
      boolean notAdmitted = failure instanceof RejectedExecutionException;
      response = codec.error(requestId, overload ? "RESOURCE_EXHAUSTED" : "UNAVAILABLE",
          notAdmitted
              ? "Gateway rejected admission; retry with the same request ID and command"
              : "Outcome unknown; retry with the same request ID and command",
          !notAdmitted);
    } else {
      try {
        response = codec.encode(requestId, result);
      } catch (RuntimeException e) {
        response = codec.error(requestId, "INTERNAL", "Result encoding failed; outcome unknown", true);
      }
    }
    // Keep the slot until the write completes, so a slow reader cannot grow an unbounded reply queue.
    write(ctx, response).addListener(ignored -> pending--);
  }

  private io.netty.channel.ChannelFuture write(ChannelHandlerContext ctx, String response) {
    if (!ctx.channel().isActive() || !ctx.channel().isWritable())
      return ctx.close();
    TextWebSocketFrame frame = new TextWebSocketFrame(response);
    if (frame.content().readableBytes() > MAX_RESPONSE_BYTES) {
      frame.release();
      return ctx.writeAndFlush(new CloseWebSocketFrame(1009, "Response too large; outcome unknown"))
          .addListener(ChannelFutureListener.CLOSE);
    }
    return ctx.writeAndFlush(frame).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    if (!ctx.channel().isWritable())
      ctx.close();
    else
      ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
    if (event instanceof IdleStateEvent)
      ctx.close();
    else
      ctx.fireUserEventTriggered(event);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
    if (failure instanceof TooLongFrameException)
      ctx.writeAndFlush(new CloseWebSocketFrame(1009, "Message exceeds 16 KiB"))
          .addListener(ChannelFutureListener.CLOSE);
    else
      ctx.close();
  }
}
