package dev.sam.exchange.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.stream.Stream;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;

class SbeCommandCodecTest {
  // Independently specified version-zero wire fixture: header, ID 42, ASK, price 12345, quantity 7.
  private static final byte[] ASK_MESSAGE = HexFormat.of()
      .parseHex("1900010001000000" + "2a00000000000000" + "02" + "3930000000000000" + "0700000000000000");

  // Version-zero cancellation: an 8-byte header followed by order ID 42.
  private static final byte[] CANCEL_MESSAGE = HexFormat.of().parseHex("0800020001000000" + "2a00000000000000");

  private final SbeCommandCodec codec = new SbeCommandCodec();

  @ParameterizedTest
  @MethodSource("commands")
  void rejectsEveryTruncatedDestinationWithoutChangingAnyBytes(EngineCommand command, byte[] expected) {
    for (int offset : new int[]{0, 7}) {
      for (int available = 0; available < expected.length; available++) {
        byte[] bytes = new byte[offset + available];
        Arrays.fill(bytes, (byte) 0x55);
        byte[] original = bytes.clone();

        assertThrows(IndexOutOfBoundsException.class, () -> codec.encode(command, new UnsafeBuffer(bytes), offset));

        assertArrayEquals(original, bytes, "offset=" + offset + ", available=" + available);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("commands")
  void rejectsInvalidOffsetsWithoutChangingAnyBytes(EngineCommand command, byte[] expected) {
    for (int offset : new int[]{-1, Integer.MIN_VALUE, Integer.MAX_VALUE - expected.length + 1, Integer.MAX_VALUE}) {
      byte[] bytes = new byte[64];
      Arrays.fill(bytes, (byte) 0x55);
      byte[] original = bytes.clone();

      assertThrows(IndexOutOfBoundsException.class, () -> codec.encode(command, new UnsafeBuffer(bytes), offset));

      assertArrayEquals(original, bytes);
    }
  }

  @ParameterizedTest
  @MethodSource("commands")
  void reportsTheCompleteFrameLength(EngineCommand command, byte[] expected) {
    assertEquals(expected.length, codec.encodedLength(command));
  }

  @ParameterizedTest
  @MethodSource("commands")
  void rejectsInvalidOffsetsWithoutGrowingAnExpandableBuffer(EngineCommand command, byte[] expected) {
    for (int offset : new int[]{-1, Integer.MIN_VALUE, Integer.MAX_VALUE - expected.length + 1, Integer.MAX_VALUE}) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(7);
      buffer.setMemory(0, 7, (byte) 0x55);

      assertThrows(IndexOutOfBoundsException.class, () -> codec.encode(command, buffer, offset));

      assertEquals(7, buffer.capacity());
      for (int i = 0; i < 7; i++) {
        assertEquals((byte) 0x55, buffer.getByte(i));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("commands")
  void encodesIntoAnExactlySizedFixedBuffer(EngineCommand command, byte[] expected) {
    for (int offset : new int[]{0, 7}) {
      byte[] bytes = new byte[offset + expected.length];
      Arrays.fill(bytes, (byte) 0x55);

      int length = codec.encode(command, new UnsafeBuffer(bytes), offset);

      assertEquals(expected.length, length);
      assertArrayEquals(expected, Arrays.copyOfRange(bytes, offset, bytes.length));
      for (int i = 0; i < offset; i++) {
        assertEquals((byte) 0x55, bytes[i]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("commands")
  void growsAnExpandableBufferForTheCompleteFrame(EngineCommand command, byte[] expected) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(7);
    buffer.setMemory(0, 7, (byte) 0x55);

    int length = codec.encode(command, buffer, 7);

    assertEquals(expected.length, length);
    byte[] encoded = new byte[length];
    buffer.getBytes(7, encoded);
    assertArrayEquals(expected, encoded);
    for (int i = 0; i < 7; i++) {
      assertEquals((byte) 0x55, buffer.getByte(i));
    }
  }

  private static Stream<Arguments> commands() {
    return Stream.of(Arguments.of(new PlaceOrder(42L, Side.ASK, 12345L, 7L), ASK_MESSAGE),
        Arguments.of(new CancelOrder(42L), CANCEL_MESSAGE));
  }

  @Test
  void encodesTheHeaderAndFieldsAtTheRequestedOffset() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.setMemory(0, 7, (byte) 0x55);
    int length = codec.encode(new PlaceOrder(42L, Side.ASK, 12345L, 7L), buffer, 7);

    assertEquals(33, length);
    byte[] encoded = new byte[length];
    buffer.getBytes(7, encoded);
    assertArrayEquals(ASK_MESSAGE, encoded);
    assertEquals((byte) 0x55, buffer.getByte(6));
  }

  @Test
  void decodesAnIndependentlyEncodedMessageAtANonzeroOffset() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.putBytes(11, ASK_MESSAGE);
    assertEquals(new PlaceOrder(42L, Side.ASK, 12345L, 7L), codec.decode(buffer, 11, ASK_MESSAGE.length));
  }

  @ParameterizedTest
  @EnumSource(Side.class)
  void roundTripsBothSidesAndFullWidthLongFields(Side side) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    PlaceOrder order = new PlaceOrder(Long.MIN_VALUE, side, Long.MAX_VALUE, Long.MAX_VALUE - 1);
    int length = codec.encode(order, buffer, 3);
    assertEquals(order, codec.decode(buffer, 3, length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 7, 8, 32})
  void rejectsTruncatedFramesEvenWhenTheBackingBufferContainsMoreBytes(int length) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(ASK_MESSAGE.clone()), 0, length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 4, 6})
  void rejectsAnUnsupportedHeader(int fieldOffset) {
    byte[] bytes = ASK_MESSAGE.clone();
    bytes[fieldOffset]++;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 255})
  void rejectsInvalidOrNullSideValues(int sideValue) {
    byte[] bytes = ASK_MESSAGE.clone();
    bytes[16] = (byte) sideValue;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @Test
  void appliesTheExistingOrderValidationAfterDecoding() {
    byte[] bytes = ASK_MESSAGE.clone();
    bytes[25] = 0; // Zero the fixture's quantity (the remaining seven quantity bytes are already zero).
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @Test
  void encodesCancellationAtTheRequestedOffset() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.setMemory(0, 7, (byte) 0x55);

    int length = codec.encode(new CancelOrder(42L), buffer, 7);

    assertEquals(16, length);
    byte[] encoded = new byte[length];
    buffer.getBytes(7, encoded);
    assertArrayEquals(CANCEL_MESSAGE, encoded);
    assertEquals((byte) 0x55, buffer.getByte(6));
  }

  @Test
  void decodesAnIndependentCancellationAtANonzeroOffset() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.putBytes(11, CANCEL_MESSAGE);

    assertEquals(new CancelOrder(42L), codec.decode(buffer, 11, CANCEL_MESSAGE.length));
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MIN_VALUE, 0L, Long.MAX_VALUE})
  void roundTripsCancellationOrderIds(long orderId) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    CancelOrder command = new CancelOrder(orderId);

    int length = codec.encode(command, buffer, 3);

    assertEquals(command, codec.decode(buffer, 3, length));
  }

  @Test
  void reusesDecodersAcrossCommandTypesBuffersAndOffsets() {
    ExpandableArrayBuffer first = new ExpandableArrayBuffer();
    first.putBytes(3, CANCEL_MESSAGE);
    assertEquals(new CancelOrder(42L), codec.decode(first, 3, CANCEL_MESSAGE.length));

    ExpandableArrayBuffer second = new ExpandableArrayBuffer();
    second.putBytes(7, ASK_MESSAGE);
    assertEquals(new PlaceOrder(42L, Side.ASK, 12345L, 7L), codec.decode(second, 7, ASK_MESSAGE.length));

    byte[] nextCancellation = CANCEL_MESSAGE.clone();
    nextCancellation[8] = 99;
    second.putBytes(41, nextCancellation);
    assertEquals(new CancelOrder(99L), codec.decode(second, 41, nextCancellation.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 7, 8, 15, 17})
  void rejectsIncorrectCancellationFrameLengthsDespiteSpareBufferCapacity(int length) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
    buffer.putBytes(0, CANCEL_MESSAGE);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 0, length));
  }

  @Test
  void rejectsPlaceOrderWithCancellationBlockLengthDespiteExtraBackingBytes() {
    byte[] bytes = ASK_MESSAGE.clone();
    bytes[0] = 8; // Claim a cancellation-sized body while leaving a full place order in the backing buffer.

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, 16));
  }

  @Test
  void rejectsCancellationWithPlaceOrderBlockLength() {
    byte[] bytes = new byte[33];
    System.arraycopy(CANCEL_MESSAGE, 0, bytes, 0, CANCEL_MESSAGE.length);
    bytes[0] = 25;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 99})
  void rejectsUnknownTemplateIds(int templateId) {
    byte[] bytes = CANCEL_MESSAGE.clone();
    bytes[2] = (byte) templateId;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }
}
