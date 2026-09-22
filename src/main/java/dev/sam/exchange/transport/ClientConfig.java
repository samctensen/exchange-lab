package dev.sam.exchange.transport;

import java.time.Duration;
import java.util.Objects;

public record ClientConfig(Duration timeout, int maxAttempts) {
  public ClientConfig {
    // Reject null, zero, or negative timeout.
    Objects.requireNonNull(timeout, "timeout must not be null");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    // Reject maxAttempts below 1.
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
  }

  public static ClientConfig defaults() {
    return new ClientConfig(Duration.ofSeconds(5), 3);
  }
}
