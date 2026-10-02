package dev.sam.exchange.gateway;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketFrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

/** Loopback learning endpoint. The caller owns and starts the supplied EngineGateway. */
public final class WebSocketGateway implements AutoCloseable {
  private static final int MAX_MESSAGE_BYTES = 16 * 1024;
  private static final int MAX_CONNECTIONS = 128;
  private final EngineGateway gateway;
  private final int requestedPort;
  private final EventLoopGroup acceptor;
  private final EventLoopGroup workers;
  private final ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final AtomicInteger connectionCount = new AtomicInteger();
  private volatile Channel listener;
  private boolean started;
  private boolean closed;

  public WebSocketGateway(EngineGateway gateway, int port) {
    this.gateway = Objects.requireNonNull(gateway);
    if (port < 0 || port > 65535)
      throw new IllegalArgumentException("Port must be 0..65535");
    requestedPort = port;
    acceptor = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    workers = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
  }

  public synchronized void start() throws IOException, InterruptedException {
    if (started || closed)
      throw new IllegalStateException("WebSocket gateway can only be started once");
    started = true;
    try {
      listener = new ServerBootstrap().group(acceptor, workers).channel(NioServerSocketChannel.class)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64 * 1024, 128 * 1024))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
              if (connectionCount.incrementAndGet() > MAX_CONNECTIONS) {
                connectionCount.decrementAndGet();
                channel.close();
                return;
              }
              clients.add(channel);
              channel.closeFuture().addListener(ignored -> connectionCount.decrementAndGet());
              var pipeline = channel.pipeline();
              pipeline.addLast(new IdleStateHandler(60, 0, 0));
              pipeline.addLast(new HttpServerCodec());
              pipeline.addLast(new HttpObjectAggregator(MAX_MESSAGE_BYTES));
              pipeline.addLast(new LocalUpgradeGuard());
              pipeline.addLast(new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder()
                  .websocketPath("/orders").maxFramePayloadLength(MAX_MESSAGE_BYTES).allowExtensions(false)
                  .handshakeTimeoutMillis(5000).build()));
              pipeline.addLast(new WebSocketFrameAggregator(MAX_MESSAGE_BYTES));
              pipeline.addLast(new WebSocketOrderHandler(gateway::submit, 32));
            }
          }).bind(new InetSocketAddress("127.0.0.1", requestedPort)).sync().channel();
    } catch (Exception failure) {
      close();
      if (failure instanceof InterruptedException interrupted)
        throw interrupted;
      if (failure instanceof IOException io)
        throw io;
      throw new IOException("Could not start WebSocket gateway", failure);
    }
  }

  public int port() {
    Channel channel = listener;
    if (channel == null)
      throw new IllegalStateException("WebSocket gateway has not started");
    return ((InetSocketAddress) channel.localAddress()).getPort();
  }

  public void awaitClose() throws InterruptedException {
    Channel channel = listener;
    if (channel == null)
      throw new IllegalStateException("WebSocket gateway has not started");
    channel.closeFuture().sync();
  }

  @Override
  public synchronized void close() {
    if (closed)
      return;
    closed = true;
    if (listener != null)
      listener.close().syncUninterruptibly();
    for (Channel channel : clients) {
      if (channel.pipeline().get(WebSocketFrameEncoder.class) != null)
        channel.writeAndFlush(new CloseWebSocketFrame(1001, "Gateway shutting down; reconcile pending requests"))
            .addListener(ChannelFutureListener.CLOSE);
      else
        channel.close();
    }
    clients.newCloseFuture().awaitUninterruptibly(1, TimeUnit.SECONDS);
    clients.close().awaitUninterruptibly();
    acceptor.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
  }

  /** Prevent an unrelated website from using a browser to submit to the unauthenticated local demo. */
  private static final class LocalUpgradeGuard extends SimpleChannelInboundHandler<FullHttpRequest> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
      HttpResponseStatus failure = null;
      if (!request.decoderResult().isSuccess())
        failure = HttpResponseStatus.BAD_REQUEST;
      else if (!request.method().equals(HttpMethod.GET) || !request.uri().equals("/orders"))
        failure = HttpResponseStatus.NOT_FOUND;
      else if (!allowedOrigin(request.headers().get(HttpHeaderNames.ORIGIN)))
        failure = HttpResponseStatus.FORBIDDEN;
      if (failure != null) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, failure))
            .addListener(ChannelFutureListener.CLOSE);
      } else {
        ctx.fireChannelRead(request.retain());
      }
    }

    private boolean allowedOrigin(String origin) {
      if (origin == null)
        return true; // Native clients do not send browser Origin headers. This is not authentication.
      try {
        URI uri = URI.create(origin);
        return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
            && ("localhost".equals(uri.getHost()) || "127.0.0.1".equals(uri.getHost())) && uri.getRawUserInfo() == null
            && uri.getRawQuery() == null && uri.getRawFragment() == null
            && (uri.getRawPath() == null || uri.getRawPath().isEmpty());
      } catch (IllegalArgumentException invalid) {
        return false;
      }
    }
  }
}
