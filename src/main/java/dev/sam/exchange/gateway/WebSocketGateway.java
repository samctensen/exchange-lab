package dev.sam.exchange.gateway;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketFrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

/** Loopback learning endpoint. The caller owns and starts the supplied EngineGateway. */
public final class WebSocketGateway implements AutoCloseable {
  private final EngineGateway gateway;
  private final int requestedPort;
  private final WebSocketLimits limits;
  private final Supplier<String> metrics;
  private final EventLoopGroup acceptor;
  private final EventLoopGroup workers;
  private final ChannelGroup clients = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final AtomicInteger connectionCount = new AtomicInteger();
  private volatile Channel listener;
  private boolean started;
  private boolean closed;

  public WebSocketGateway(EngineGateway gateway, int port) {
    this(gateway, port, WebSocketLimits.defaults());
  }

  public WebSocketGateway(EngineGateway gateway, int port, WebSocketLimits limits) {
    this(gateway, port, limits, null);
  }

  WebSocketGateway(EngineGateway gateway, int port, WebSocketLimits limits, Supplier<String> metrics) {
    this.gateway = Objects.requireNonNull(gateway);
    this.limits = Objects.requireNonNull(limits, "limits");
    this.metrics = metrics;
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
              if (connectionCount.incrementAndGet() > limits.maxConnections()) {
                connectionCount.decrementAndGet();
                channel.close();
                return;
              }
              clients.add(channel);
              channel.closeFuture().addListener(ignored -> connectionCount.decrementAndGet());
              configurePipeline(channel.pipeline());
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

  void configurePipeline(ChannelPipeline pipeline) {
    pipeline.addLast(new IdleStateHandler(limits.readIdleTimeoutMillis(), 0, 0, TimeUnit.MILLISECONDS));
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(limits.maxMessageBytes()));
    pipeline.addLast(new GatewayHttpHandler(gateway::isReady, metrics));
    pipeline.addLast(new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder()
        .websocketPath("/orders").maxFramePayloadLength(limits.maxMessageBytes()).allowExtensions(false)
        .handshakeTimeoutMillis(limits.handshakeTimeoutMillis()).build()));
    pipeline.addLast(new WebSocketFrameAggregator(limits.maxMessageBytes()));
    pipeline.addLast(new WebSocketOrderHandler(gateway::submit, limits));
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

}
