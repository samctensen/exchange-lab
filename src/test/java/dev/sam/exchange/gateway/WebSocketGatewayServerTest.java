package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WebSocketGatewayServerTest {
  @ParameterizedTest
  @ValueSource(strings = {"--port=-1", "--port=65536", "--queue-capacity=0", "--queue-capacity=-1", "--max-in-flight=0",
      "--max-in-flight=-1", "--max-in-flight=oops", "--port=", "--host=0.0.0.0", "--unknown=1", "--diagnostics=true",
      "--port"})
  void rejectsInvalidFlagsBeforeConnectingToAeron(String flag) {
    assertThrows(IllegalArgumentException.class, () -> WebSocketGatewayServer.main(new String[]{flag}));
  }

  @Test
  void rejectsDuplicateFlagsBeforeConnectingToAeron() {
    assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.main(new String[]{"--port=8080", "--port=8081"}));
  }

  @Test
  void configHasLocalDemoDefaultsAndSupportsExplicitLimits() {
    assertEquals(new WebSocketGatewayServer.Config(8080, 128, 8), WebSocketGatewayServer.Config.parse(new String[0]));
    assertEquals(new WebSocketGatewayServer.Config(0, 16, 4),
        WebSocketGatewayServer.Config.parse(new String[]{"--port=0", "--queue-capacity=16", "--max-in-flight=4"}));
  }
}
