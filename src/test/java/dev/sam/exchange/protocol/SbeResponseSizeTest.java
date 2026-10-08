package dev.sam.exchange.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.transport.CommandResponse;

class SbeResponseSizeTest {
  private static final UUID REQUEST_ID = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
  private static final Trade TRADE = new Trade(42L, 7L, 101L, 4L);
  private final SbeResponseCodec codec = new SbeResponseCodec();

  static Stream<Arguments> responseLengths() {
    // Independent wire sizes: eight header bytes, then 25 fixed bytes for cancel/reject;
    // placement has 32 fixed bytes, a four-byte group header, and 32 bytes per trade.
    return Stream.of(Arguments.of(new CommandResponse(REQUEST_ID, new CancelResult(42L, true)), 33),
        Arguments.of(new CommandResponse(REQUEST_ID, new CancelResult(42L, false)), 33),
        Arguments.of(new CommandResponse(REQUEST_ID, new RejectResult(42L, RejectReason.DUPLICATE_ORDER_ID)), 33),
        Arguments.of(new CommandResponse(REQUEST_ID, new RejectResult(42L, RejectReason.REQUEST_ID_CONFLICT)), 33),
        Arguments.of(new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(), 10L)), 44),
        Arguments.of(new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(TRADE), 0L)), 76),
        Arguments.of(
            new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(TRADE, new Trade(42L, 8L, 100L, 6L)), 2L)),
            108));
  }

  @ParameterizedTest
  @MethodSource("responseLengths")
  void predictsTheCompleteFrameLengthIndependentOfDestinationOffset(CommandResponse response, int expected) {
    assertEquals(expected, codec.encodedLength(response));

    for (int offset : new int[]{0, 7}) {
      byte[] bytes = new byte[offset + expected + 1];
      Arrays.fill(bytes, (byte) 0x55);
      UnsafeBuffer buffer = new UnsafeBuffer(bytes);

      int actual = codec.encode(response, buffer, offset);

      assertEquals(expected, actual);
      assertEquals(codec.encodedLength(response), actual);
      assertEquals(response, codec.decode(buffer, offset, actual));
      assertEquals((byte) 0x55, bytes[offset + actual]);
      if (offset > 0) {
        assertEquals((byte) 0x55, bytes[offset - 1]);
      }
    }
  }

  @Test
  void supportsTheLargestTradeGroupAllowedByTheSchema() {
    CommandResponse response = new CommandResponse(REQUEST_ID,
        new PlaceResult(42L, Collections.nCopies(65534, TRADE), 0L));
    UnsafeBuffer buffer = new UnsafeBuffer(new byte[2097132]);

    assertEquals(2097132, codec.encodedLength(response));
    assertEquals(2097132, codec.encode(response, buffer, 0));
    assertEquals(response, codec.decode(buffer, 0, 2097132));
  }

  @ParameterizedTest
  @ValueSource(ints = {65535, 65536})
  void rejectsTradeCountsThatCannotBeRepresentedInTheWireSize(int tradeCount) {
    CommandResponse response = new CommandResponse(REQUEST_ID,
        new PlaceResult(42L, Collections.nCopies(tradeCount, TRADE), 0L));

    assertThrows(IllegalArgumentException.class, () -> codec.encodedLength(response));
  }

  @ParameterizedTest
  @ValueSource(ints = {65535, 65536})
  void rejectsUnencodableTradeCountsBeforeChangingTheDestination(int tradeCount) {
    byte[] bytes = new byte[128];
    Arrays.fill(bytes, (byte) 0x55);
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    codec.encode(new CommandResponse(REQUEST_ID, new CancelResult(42L, true)), buffer, 7);
    byte[] original = bytes.clone();
    CommandResponse oversized = new CommandResponse(REQUEST_ID,
        new PlaceResult(42L, Collections.nCopies(tradeCount, TRADE), 0L));

    assertThrows(IllegalArgumentException.class, () -> codec.encode(oversized, buffer, 7));

    assertArrayEquals(original, bytes);
  }
}
