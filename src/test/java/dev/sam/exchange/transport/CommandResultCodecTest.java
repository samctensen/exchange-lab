package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Trade;

class CommandResultCodecTest {

  private final CommandResultCodec codec = new CommandResultCodec();

  @ParameterizedTest
  @MethodSource("validResults")
  void encodesResultUsingExpectedWireFormat(CommandResult result, String encoded) {
    assertEquals(encoded, codec.encode(result));
  }

  @ParameterizedTest
  @MethodSource("validResults")
  void decodesResultFromExpectedWireFormat(CommandResult result, String encoded) {
    assertEquals(result, codec.decode(encoded));
  }

  private static Stream<Arguments> validResults() {
    // Literal messages check the protocol independently of the encoder and decoder.
    return Stream.of(Arguments.of(new CancelResult(1L, true), "CANCEL_RESULT,1,true"),
        Arguments.of(new CancelResult(2L, false), "CANCEL_RESULT,2,false"),
        Arguments.of(new PlaceResult(1L, List.of(), 10L), "PLACE_RESULT,1,10,0"),
        Arguments.of(new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L), "PLACE_RESULT,2,0,1,2,1,100,4"),
        Arguments.of(new PlaceResult(3L, List.of(new Trade(3L, 1L, 101L, 4L), new Trade(3L, 2L, 100L, 5L)), 2L),
            "PLACE_RESULT,3,2,2,3,1,101,4,3,2,100,5"),
        Arguments.of(new CancelResult(Long.MAX_VALUE, false), "CANCEL_RESULT,9223372036854775807,false"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"TRUE", "FALSE", "yes", ""})
  void rejectsInvalidCancelledFlag(String flag) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode("CANCEL_RESULT,1," + flag));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "UNKNOWN,1", "CANCEL_RESULT,1", "CANCEL_RESULT,1,true,", "PLACE_RESULT,1,10",
      "PLACE_RESULT,1,10,-1", "PLACE_RESULT,2,0,1", "PLACE_RESULT,2,0,0,2,1,100,4", "PLACE_RESULT,1,10,0,",
      "PLACE_RESULT,2,0,1,2,1,100", "PLACE_RESULT,1,10,nope", "PLACE_RESULT,2,0,1,2,1,100,nope"})
  void rejectsMalformedResult(String encoded) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
  }

  @ParameterizedTest
  @ValueSource(strings = {"PLACE_RESULT,1,10,1073741824", "PLACE_RESULT,2,0,1073741825,2,1,100,4"})
  void rejectsTradeCountThatWouldOverflowIntFieldCalculation(String encoded) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded));
  }
}
