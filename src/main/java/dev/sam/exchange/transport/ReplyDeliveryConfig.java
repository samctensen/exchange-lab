package dev.sam.exchange.transport;

import java.time.Duration;
import java.util.Objects;

/** Bounds waiting replies per Aeron client connection; recording has its own deadline. */
public record ReplyDeliveryConfig(int maxReplies, long maxBytes, Duration timeout) {
  public ReplyDeliveryConfig {
    if (maxReplies <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Reply count and byte limits must be positive");
    }
    Objects.requireNonNull(timeout, "Reply timeout must not be null");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("Reply timeout must be positive");
    }
    try {
      timeout.toNanos();
    } catch (ArithmeticException overflow) {
      throw new IllegalArgumentException("Reply timeout must fit in nanoseconds", overflow);
    }
  }

  public static ReplyDeliveryConfig defaults() {
    return new ReplyDeliveryConfig(64, 64 * 1024L, Duration.ofSeconds(5));
  }
}
