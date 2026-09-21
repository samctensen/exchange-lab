package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Trade;

class CommandResponseCodecTest {
  private final CommandResponseCodec codec = new CommandResponseCodec();

  @ParameterizedTest
  @MethodSource("validResponses")
  void encodesResponseUsingExpectedWireFormat(CommandResponse response, String encoded) {
    assertEquals(encoded, codec.encode(response));
  }

  @ParameterizedTest
  @MethodSource("validResponses")
  void decodesResponseFromExpectedWireFormat(CommandResponse response, String encoded) {
    assertEquals(response, codec.decode(encoded));
  }

  private static Stream<Arguments> validResponses() {
    UUID firstId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    UUID secondId = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
    // Check the UUID and the entire payload, including every comma in a multi-trade result.
    return Stream.of(
        Arguments.of(new CommandResponse(firstId, new CancelResult(1L, true)),
            "RESPONSE,550e8400-e29b-41d4-a716-446655440000,CANCEL_RESULT,1,true"),
        Arguments.of(new CommandResponse(secondId, new CancelResult(2L, false)),
            "RESPONSE,f47ac10b-58cc-4372-a567-0e02b2c3d479,CANCEL_RESULT,2,false"),
        Arguments.of(new CommandResponse(firstId, new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID)),
            "RESPONSE,550e8400-e29b-41d4-a716-446655440000,REJECT_RESULT,1,DUPLICATE_ORDER_ID"),
        Arguments.of(new CommandResponse(secondId, new PlaceResult(1L, List.of(), 10L)),
            "RESPONSE,f47ac10b-58cc-4372-a567-0e02b2c3d479,PLACE_RESULT,1,10,0"),
        Arguments.of(
            new CommandResponse(firstId,
                new PlaceResult(3L, List.of(new Trade(3L, 1L, 101L, 4L), new Trade(3L, 2L, 100L, 5L)), 2L)),
            "RESPONSE,550e8400-e29b-41d4-a716-446655440000,PLACE_RESULT,3,2,2,3,1,101,4,3,2,100,5"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "RESPONSE", "RESPONSE,550e8400-e29b-41d4-a716-446655440000",
      "REQUEST,550e8400-e29b-41d4-a716-446655440000,CANCEL_RESULT,1,true",
      "response,550e8400-e29b-41d4-a716-446655440000,CANCEL_RESULT,1,true", "RESPONSE,nope,CANCEL_RESULT,1,true",
      "RESPONSE,,CANCEL_RESULT,1,true", "RESPONSE,550e8400-e29b-41d4-a716-446655440000,",
      "RESPONSE,550e8400-e29b-41d4-a716-446655440000,UNKNOWN,1",
      "RESPONSE,550e8400-e29b-41d4-a716-446655440000,CANCEL_RESULT,1,yes",
      "RESPONSE,550e8400-e29b-41d4-a716-446655440000,PLACE_RESULT,2,0,1",
      "RESPONSE,550e8400-e29b-41d4-a716-446655440000,REJECT_RESULT,1,DUPLICATE_ORDER_ID,"})
  void rejectsMalformedResponse(String encoded) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
  }
}
