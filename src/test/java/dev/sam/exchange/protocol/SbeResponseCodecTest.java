package dev.sam.exchange.protocol;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.transport.CommandResponse;

class SbeResponseCodecTest {
  private static final UUID REQUEST_ID = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
  // Independent little-endian fixture: header, UUID halves, order ID, then the cancellation flag.
  private static final byte[] CANCELLED_MESSAGE = HexFormat.of()
      .parseHex("1900060001000000" + "efcdab8967452301" + "1032547698badcfe" + "2a00000000000000" + "01");
  private static final byte[] REJECTED_MESSAGE = HexFormat.of()
      .parseHex("1900070001000000" + "efcdab8967452301" + "1032547698badcfe" + "2a00000000000000" + "01");
  // Placement header and fixed fields precede the four-byte group header and 32-byte trade entries.
  private static final String PLACE_PREFIX = "2000050001000000" + "efcdab8967452301" + "1032547698badcfe"
      + "2a00000000000000";
  private static final String FIRST_TRADE = "2a00000000000000" + "0700000000000000" + "6500000000000000"
      + "0400000000000000";
  private static final String SECOND_TRADE = "2a00000000000000" + "0800000000000000" + "6400000000000000"
      + "0600000000000000";
  private static final byte[] EMPTY_PLACE_MESSAGE = HexFormat.of()
      .parseHex(PLACE_PREFIX + "0a00000000000000" + "20000000");
  private static final byte[] ONE_TRADE_MESSAGE = HexFormat.of()
      .parseHex(PLACE_PREFIX + "0000000000000000" + "20000100" + FIRST_TRADE);
  private static final byte[] TWO_TRADE_MESSAGE = HexFormat.of()
      .parseHex(PLACE_PREFIX + "0200000000000000" + "20000200" + FIRST_TRADE + SECOND_TRADE);

  private final SbeResponseCodec codec = new SbeResponseCodec();

  static Stream<Arguments> wireMessages() {
    byte[] notCancelled = CANCELLED_MESSAGE.clone();
    notCancelled[32] = 0;
    byte[] conflict = REJECTED_MESSAGE.clone();
    conflict[32] = 2;
    return Stream.of(Arguments.of(new CommandResponse(REQUEST_ID, new CancelResult(42L, true)), CANCELLED_MESSAGE),
        Arguments.of(new CommandResponse(REQUEST_ID, new CancelResult(42L, false)), notCancelled),
        Arguments.of(new CommandResponse(REQUEST_ID, new RejectResult(42L, RejectReason.DUPLICATE_ORDER_ID)),
            REJECTED_MESSAGE),
        Arguments.of(new CommandResponse(REQUEST_ID, new RejectResult(42L, RejectReason.REQUEST_ID_CONFLICT)),
            conflict));
  }

  static Stream<Arguments> placeWireMessages() {
    Trade first = new Trade(42L, 7L, 101L, 4L);
    Trade second = new Trade(42L, 8L, 100L, 6L);
    return Stream.of(
        Arguments.of(new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(), 10L)), EMPTY_PLACE_MESSAGE),
        Arguments.of(new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(first), 0L)), ONE_TRADE_MESSAGE),
        Arguments.of(new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(first, second), 2L)),
            TWO_TRADE_MESSAGE));
  }

  @ParameterizedTest
  @MethodSource({"wireMessages", "placeWireMessages"})
  void encodesTheExpectedWireBytesAtANonzeroOffset(CommandResponse response, byte[] expected) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    buffer.setMemory(0, 128, (byte) 0x55);

    int length = codec.encode(response, buffer, 7);

    assertEquals(expected.length, length);
    byte[] actual = new byte[length];
    buffer.getBytes(7, actual);
    assertArrayEquals(expected, actual);
    assertEquals((byte) 0x55, buffer.getByte(6));
    assertEquals((byte) 0x55, buffer.getByte(7 + length));
  }

  @Test
  void resetsTradeCountAndLengthWhenReusingEncoder() {
    ExpandableArrayBuffer first = new ExpandableArrayBuffer(128);
    ExpandableArrayBuffer second = new ExpandableArrayBuffer(128);
    CommandResponse filled = new CommandResponse(REQUEST_ID,
        new PlaceResult(42L, List.of(new Trade(42L, 7L, 101L, 4L), new Trade(42L, 8L, 100L, 6L)), 2L));
    CommandResponse empty = new CommandResponse(REQUEST_ID, new PlaceResult(42L, List.of(), 10L));

    assertEquals(108, codec.encode(filled, first, 3));
    assertEquals(44, codec.encode(empty, second, 11));
    byte[] actual = new byte[44];
    second.getBytes(11, actual);
    assertArrayEquals(EMPTY_PLACE_MESSAGE, actual);

    // Shrinking a message in the same buffer must also reset the encoded count and length.
    assertEquals(44, codec.encode(empty, first, 3));
    first.getBytes(3, actual);
    assertArrayEquals(EMPTY_PLACE_MESSAGE, actual);
    assertEquals(108, codec.encode(filled, second, 11));
    byte[] expanded = new byte[108];
    second.getBytes(11, expanded);
    assertArrayEquals(TWO_TRADE_MESSAGE, expanded);
  }

  @ParameterizedTest
  @MethodSource({"wireMessages", "placeWireMessages"})
  void decodesAnIndependentWireMessageAtANonzeroOffset(CommandResponse expected, byte[] message) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    buffer.putBytes(11, message);

    assertEquals(expected, codec.decode(buffer, 11, message.length));
  }

  static Stream<UUID> requestIds() {
    return Stream.of(new UUID(Long.MIN_VALUE, Long.MAX_VALUE), new UUID(Long.MAX_VALUE, Long.MIN_VALUE),
        new UUID(0L, 0L), new UUID(-1L, -1L));
  }

  @ParameterizedTest
  @MethodSource("requestIds")
  void roundTripsAllUuidAndOrderIdBits(UUID requestId) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    for (long orderId : new long[]{Long.MIN_VALUE, 0L, Long.MAX_VALUE}) {
      for (CommandResult result : List.of(new CancelResult(orderId, false), new CancelResult(orderId, true),
          new RejectResult(orderId, RejectReason.DUPLICATE_ORDER_ID),
          new RejectResult(orderId, RejectReason.REQUEST_ID_CONFLICT), new PlaceResult(orderId, List.of(), 0L),
          new PlaceResult(orderId, List.of(new Trade(orderId, ~orderId, Long.MAX_VALUE, Long.MAX_VALUE)),
              Long.MAX_VALUE))) {
        CommandResponse response = new CommandResponse(requestId, result);

        int length = codec.encode(response, buffer, 3);

        assertEquals(response, codec.decode(buffer, 3, length));
      }
    }
  }

  @Test
  void reusesCodecsAcrossBuffersAndOffsetsWithoutChangingEarlierResponses() {
    ExpandableArrayBuffer first = new ExpandableArrayBuffer(128);
    first.putBytes(3, CANCELLED_MESSAGE);
    CommandResponse decoded = codec.decode(first, 3, CANCELLED_MESSAGE.length);
    ExpandableArrayBuffer second = new ExpandableArrayBuffer(128);

    CommandResponse notCancelled = new CommandResponse(new UUID(0L, -1L), new CancelResult(99L, false));
    int length = codec.encode(notCancelled, second, 7);
    assertEquals(notCancelled, codec.decode(second, 7, length));

    CommandResponse rejected = new CommandResponse(new UUID(Long.MIN_VALUE, Long.MAX_VALUE),
        new RejectResult(-1L, RejectReason.REQUEST_ID_CONFLICT));
    length = codec.encode(rejected, first, 3);
    CommandResponse decodedRejection = codec.decode(first, 3, length);
    assertEquals(rejected, decodedRejection);

    CommandResponse placed = new CommandResponse(new UUID(Long.MAX_VALUE, Long.MIN_VALUE),
        new PlaceResult(42L, List.of(new Trade(42L, -1L, 101L, 4L)), 0L));
    length = codec.encode(placed, first, 9);
    CommandResponse decodedPlacement = codec.decode(first, 9, length);
    assertEquals(placed, decodedPlacement);

    CommandResponse resting = new CommandResponse(REQUEST_ID, new PlaceResult(43L, List.of(), 5L));
    length = codec.encode(resting, second, 11);
    assertEquals(resting, codec.decode(second, 11, length));

    CommandResponse cancelled = new CommandResponse(new UUID(-1L, 0L), new CancelResult(100L, true));
    length = codec.encode(cancelled, first, 17);
    assertEquals(cancelled, codec.decode(first, 17, length));
    assertEquals(new CommandResponse(REQUEST_ID, new CancelResult(42L, true)), decoded);
    assertEquals(rejected, decodedRejection);
    assertEquals(placed, decodedPlacement);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 7, 8, 32, 34})
  void rejectsIncorrectFrameLengthsDespiteSpareBufferCapacity(int length) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    for (byte[] message : List.of(CANCELLED_MESSAGE, REJECTED_MESSAGE)) {
      buffer.putBytes(3, message);
      assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 3, length));
    }
  }

  @ParameterizedTest
  @CsvSource({"-1, 33", "110, 33", "128, 33", "2147483647, 33", "3, 2147483647"})
  void rejectsFramesOutsideTheBuffer(int offset, int length) {
    UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, offset, length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 24, 26})
  void rejectsIncorrectBlockLengthsEvenWhenTheFrameLengthMatches(int blockLength) {
    for (byte[] message : List.of(CANCELLED_MESSAGE, REJECTED_MESSAGE)) {
      byte[] bytes = Arrays.copyOf(message, 8 + blockLength);
      bytes[0] = (byte) blockLength;
      assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 99})
  void rejectsUnknownAndRequestTemplateIds(int templateId) {
    byte[] bytes = CANCELLED_MESSAGE.clone();
    bytes[2] = (byte) templateId;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @CsvSource({"4, 0", "4, 2", "6, 1"})
  void rejectsUnsupportedSchemaOrVersion(int fieldOffset, int value) {
    for (byte[] message : List.of(CANCELLED_MESSAGE, REJECTED_MESSAGE, EMPTY_PLACE_MESSAGE, TWO_TRADE_MESSAGE)) {
      byte[] bytes = message.clone();
      bytes[fieldOffset] = (byte) value;
      assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 254, 255})
  void rejectsInvalidOrNullCancellationFlags(int flag) {
    byte[] bytes = CANCELLED_MESSAGE.clone();
    bytes[32] = (byte) flag;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 254, 255})
  void rejectsInvalidOrNullRejectionReasons(int reason) {
    byte[] bytes = REJECTED_MESSAGE.clone();
    bytes[32] = (byte) reason;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @MethodSource("placeWireMessages")
  void rejectsTruncatedOrTrailingPlacementBytesDespiteSpareCapacity(CommandResponse response, byte[] message) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    buffer.putBytes(7, message);
    // The full valid message remains in memory; the supplied frame length is the boundary.
    for (int length = 0; length < message.length; length++) {
      int frameLength = length;
      assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 7, frameLength),
          "Accepted truncated placement at length " + frameLength);
    }
    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 7, message.length + 1));
    assertEquals(response, codec.decode(buffer, 7, message.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 31, 33, 65535})
  void rejectsInvalidTradeBlockLengthsEvenForAnEmptyGroup(int blockLength) {
    UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_PLACE_MESSAGE.clone());
    buffer.putShort(40, (short) blockLength, LITTLE_ENDIAN);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 3, 65534})
  void rejectsTradeCountsThatDoNotMatchTheFrame(int count) {
    UnsafeBuffer buffer = new UnsafeBuffer(TWO_TRADE_MESSAGE.clone());
    buffer.putShort(42, (short) count, LITTLE_ENDIAN);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
  }

  @Test
  void rejectsReservedTradeCountEvenWhenAllEntriesFit() {
    int count = 65535;
    UnsafeBuffer buffer = new UnsafeBuffer(Arrays.copyOf(EMPTY_PLACE_MESSAGE, 44 + count * 32));
    buffer.putShort(42, (short) count, LITTLE_ENDIAN);
    for (int index = 0; index < count; index++) {
      buffer.putBytes(44 + index * 32, ONE_TRADE_MESSAGE, 44, 32);
    }

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, Long.MIN_VALUE})
  void rejectsNegativeRemainingLots(long remainingLots) {
    UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_PLACE_MESSAGE.clone());
    buffer.putLong(32, remainingLots, LITTLE_ENDIAN);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
  }

  @ParameterizedTest
  @CsvSource({"60, 0", "60, -1", "60, -9223372036854775808", "68, 0", "68, -1", "68, -9223372036854775808", "92, 0",
      "92, -1", "100, 0", "100, -1"})
  void rejectsNonpositiveTradePricesAndQuantities(int fieldOffset, long value) {
    UnsafeBuffer buffer = new UnsafeBuffer(TWO_TRADE_MESSAGE.clone());
    buffer.putLong(fieldOffset, value, LITTLE_ENDIAN);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
  }

  @Test
  void decodesAValidPlacementAfterAnInvalidTradeGroup() {
    UnsafeBuffer buffer = new UnsafeBuffer(TWO_TRADE_MESSAGE.clone());
    buffer.putShort(42, (short) 3, LITTLE_ENDIAN);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, buffer.capacity()));
    buffer.putShort(42, (short) 2, LITTLE_ENDIAN);

    assertEquals(
        new CommandResponse(REQUEST_ID,
            new PlaceResult(42L, List.of(new Trade(42L, 7L, 101L, 4L), new Trade(42L, 8L, 100L, 6L)), 2L)),
        codec.decode(buffer, 0, buffer.capacity()));
  }
}
