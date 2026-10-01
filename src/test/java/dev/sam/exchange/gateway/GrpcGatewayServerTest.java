package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GrpcGatewayServerTest {
  @ParameterizedTest
  @ValueSource(strings = {"--port=-1", "--port=65536", "--queue-capacity=0", "--queue-capacity=-1", "--max-in-flight=0",
      "--max-in-flight=-1", "--max-in-flight=oops", "--port=", "--unknown=1", "--diagnostics=true"})
  void rejectsInvalidFlagsBeforeConnectingToAeron(String argument) {
    assertThrows(IllegalArgumentException.class, () -> GrpcGatewayServer.main(new String[]{argument}));
  }

  @Test
  void rejectsDuplicateLimitsBeforeConnectingToAeron() {
    assertThrows(IllegalArgumentException.class,
        () -> GrpcGatewayServer.main(new String[]{"--max-in-flight=8", "--max-in-flight=32"}));
  }
}
