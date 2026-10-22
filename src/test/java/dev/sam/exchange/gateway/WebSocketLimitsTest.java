package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WebSocketLimitsTest {
  @Test
  void defaultsPreserveTheExistingResourceBudget() {
    assertEquals(new WebSocketLimits(128, 32, 16384, 1048576, 60000, 5000), WebSocketLimits.defaults());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void rejectsNonPositiveResourceLimits(int invalid) {
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(invalid, 1, 1, 1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(1, invalid, 1, 1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(1, 1, invalid, 1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(1, 1, 1, invalid, 1, 1));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE, 9223372036855L, Long.MAX_VALUE})
  void rejectsTimeoutsThatCannotBecomePositiveNanoseconds(long invalid) {
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(1, 1, 1, 1, invalid, 1));
    assertThrows(IllegalArgumentException.class, () -> new WebSocketLimits(1, 1, 1, 1, 1, invalid));
  }

  @Test
  void acceptsTheFullPositiveResourceRangeAndRepresentableTimeoutBoundary() {
    assertDoesNotThrow(() -> new WebSocketLimits(1, 1, 1, 1, 1, 1));
    assertDoesNotThrow(() -> new WebSocketLimits(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
        Integer.MAX_VALUE, 9223372036854L, 9223372036854L));
  }

  @Test
  void gatewayRejectsMissingDependenciesAndInvalidPortsBeforeStarting() {
    try (var gateway = new EngineGateway(new ReplyingAeronClient(request -> null), 1)) {
      assertThrows(NullPointerException.class, () -> new WebSocketGateway(null, 0, WebSocketLimits.defaults()));
      assertThrows(NullPointerException.class, () -> new WebSocketGateway(gateway, 0, null));
      assertThrows(IllegalArgumentException.class, () -> new WebSocketGateway(gateway, -1, WebSocketLimits.defaults()));
      assertThrows(IllegalArgumentException.class, () -> new WebSocketGateway(gateway, 65536, WebSocketLimits.defaults()));
    }
  }
}
