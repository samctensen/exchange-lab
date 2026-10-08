package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
  @MethodSource("overflowingTimeouts")
  void rejectsTimeoutThatCannotBeRepresentedInNanoseconds(Duration timeout) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> new ClientConfig(timeout, 1));

    assertTrue(error.getMessage().contains("timeout"));
    assertInstanceOf(ArithmeticException.class, error.getCause());
  }

  private static Stream<Duration> overflowingTimeouts() {
    return Stream.of(Duration.ofNanos(Long.MAX_VALUE).plusNanos(1), Duration.ofSeconds(Long.MAX_VALUE));
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

  @Test
  void acceptsLargestTimeoutRepresentableInNanoseconds() {
    assertDoesNotThrow(() -> new ClientConfig(Duration.ofNanos(Long.MAX_VALUE), 1));
  }
}
