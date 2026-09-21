package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;

class CommandRequestCodecTest {
  private final CommandRequestCodec codec = new CommandRequestCodec();

  @ParameterizedTest
  @MethodSource("validRequests")
  void encodesRequestUsingExpectedWireFormat(CommandRequest request, String encoded) {
    assertEquals(encoded, codec.encode(request));
  }

  @ParameterizedTest
  @MethodSource("validRequests")
  void decodesRequestFromExpectedWireFormat(CommandRequest request, String encoded) {
    assertEquals(request, codec.decode(encoded));
  }

  private static Stream<Arguments> validRequests() {
    UUID firstId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    UUID secondId = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
    // Different IDs and literal messages catch lost IDs and accidental changes to the inner command.
    return Stream.of(
        Arguments.of(new CommandRequest(firstId, new PlaceOrder(1L, Side.BID, 100L, 10L)),
            "REQUEST,550e8400-e29b-41d4-a716-446655440000,PLACE,1,BID,100,10"),
        Arguments.of(new CommandRequest(secondId, new PlaceOrder(2L, Side.ASK, 99L, 4L)),
            "REQUEST,f47ac10b-58cc-4372-a567-0e02b2c3d479,PLACE,2,ASK,99,4"),
        Arguments.of(new CommandRequest(secondId, new CancelOrder(1L)),
            "REQUEST,f47ac10b-58cc-4372-a567-0e02b2c3d479,CANCEL,1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "REQUEST", "REQUEST,550e8400-e29b-41d4-a716-446655440000",
      "RESPONSE,550e8400-e29b-41d4-a716-446655440000,CANCEL,1", "request,550e8400-e29b-41d4-a716-446655440000,CANCEL,1",
      "REQUEST,nope,CANCEL,1", "REQUEST,,CANCEL,1", "REQUEST,550e8400-e29b-41d4-a716-446655440000,",
      "REQUEST,550e8400-e29b-41d4-a716-446655440000,UNKNOWN,1",
      "REQUEST,550e8400-e29b-41d4-a716-446655440000,PLACE,1,BID,100",
      "REQUEST,550e8400-e29b-41d4-a716-446655440000,CANCEL,1,"})
  void rejectsMalformedRequest(String encoded) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
  }
}
