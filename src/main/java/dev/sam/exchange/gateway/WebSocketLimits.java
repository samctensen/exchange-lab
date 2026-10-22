package dev.sam.exchange.gateway;

/** Resource budgets for a loopback WebSocket listener and its connections. */
public record WebSocketLimits(int maxConnections, int maxPendingPerConnection, int maxMessageBytes,
    int maxResponseBytes, long readIdleTimeoutMillis, long handshakeTimeoutMillis) {
  public WebSocketLimits {
    if (maxConnections <= 0)
      throw new IllegalArgumentException("maxConnections must be positive");
    if (maxPendingPerConnection <= 0)
      throw new IllegalArgumentException("maxPendingPerConnection must be positive");
    if (maxMessageBytes <= 0)
      throw new IllegalArgumentException("maxMessageBytes must be positive");
    if (maxResponseBytes <= 0)
      throw new IllegalArgumentException("maxResponseBytes must be positive");
    if (readIdleTimeoutMillis <= 0 || readIdleTimeoutMillis > Long.MAX_VALUE / 1_000_000)
      throw new IllegalArgumentException("readIdleTimeoutMillis must be positive and fit in nanoseconds");
    if (handshakeTimeoutMillis <= 0 || handshakeTimeoutMillis > Long.MAX_VALUE / 1_000_000)
      throw new IllegalArgumentException("handshakeTimeoutMillis must be positive and fit in nanoseconds");
  }

  public static WebSocketLimits defaults() {
    return new WebSocketLimits(128, 32, 16384, 1048576, 60000, 5000);
  }
}
