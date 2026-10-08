package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReplyDeliveryConfigTest {
  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void rejectsQueuesThatCannotHoldAReply(int capacity) {
    assertThrows(IllegalArgumentException.class, () -> new ReplyDeliveryConfig(capacity, 1024, Duration.ofSeconds(5)));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void rejectsNonPositiveByteBudgets(long bytes) {
    assertThrows(IllegalArgumentException.class, () -> new ReplyDeliveryConfig(64, bytes, Duration.ofSeconds(5)));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void rejectsNonPositiveDeadlines(long nanos) {
    assertThrows(IllegalArgumentException.class, () -> new ReplyDeliveryConfig(64, 1024, Duration.ofNanos(nanos)));
  }

  @Test
  void rejectsMissingOrUnrepresentableDeadlines() {
    assertThrows(NullPointerException.class, () -> new ReplyDeliveryConfig(64, 1024, null));
    assertThrows(IllegalArgumentException.class,
        () -> new ReplyDeliveryConfig(64, 1024, Duration.ofSeconds(Long.MAX_VALUE)));
  }

  @Test
  void acceptsPositiveBoundsIncludingWideByteAccounting() {
    assertDoesNotThrow(() -> new ReplyDeliveryConfig(1, 1, Duration.ofNanos(1)));
    assertDoesNotThrow(
        () -> new ReplyDeliveryConfig(Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofNanos(Long.MAX_VALUE)));
  }
}
