package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClientConfigTest {
  @Test
  void rejectsNullTimeout() {
    assertThrows(NullPointerException.class, () -> new ClientConfig(null, 1));
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -1L, -1_000_000L})
  void rejectsNonPositiveTimeout(long nanos) {
    assertThrows(IllegalArgumentException.class, () -> new ClientConfig(Duration.ofNanos(nanos), 1));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void rejectsNonPositiveAttemptLimit(int attempts) {
    assertThrows(IllegalArgumentException.class, () -> new ClientConfig(Duration.ofMillis(100), attempts));
  }

  @Test
  void acceptsSmallestPositiveTimeoutAndOneAttempt() {
    assertDoesNotThrow(() -> new ClientConfig(Duration.ofNanos(1), 1));
  }
}
