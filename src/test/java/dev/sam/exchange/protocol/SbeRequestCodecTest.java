package dev.sam.exchange.protocol;

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

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;

class SbeRequestCodecTest {
  private static final UUID REQUEST_ID = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
  // Independent little-endian fixtures: header, both UUID halves, then the command fields.
  private static final byte[] PLACE_MESSAGE = HexFormat.of().parseHex("2900030001000000" + "efcdab8967452301"
      + "1032547698badcfe" + "2a00000000000000" + "02" + "3930000000000000" + "0700000000000000");
  private static final byte[] CANCEL_MESSAGE = HexFormat.of()
      .parseHex("1800040001000000" + "efcdab8967452301" + "1032547698badcfe" + "2a00000000000000");

  private final SbeRequestCodec codec = new SbeRequestCodec();

  static Stream<Arguments> wireMessages() {
    return Stream.of(
        Arguments.of(new CommandRequest(REQUEST_ID, new PlaceOrder(42L, Side.ASK, 12345L, 7L)), PLACE_MESSAGE),
        Arguments.of(new CommandRequest(REQUEST_ID, new CancelOrder(42L)), CANCEL_MESSAGE));
  }

  @ParameterizedTest
  @MethodSource("wireMessages")
  void encodesTheExpectedWireBytesAtANonzeroOffset(CommandRequest request, byte[] expected) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    buffer.setMemory(0, 128, (byte) 0x55);

    int length = codec.encode(request, buffer, 7);

    assertEquals(expected.length, length);
    byte[] actual = new byte[length];
    buffer.getBytes(7, actual);
    assertArrayEquals(expected, actual);
    assertEquals((byte) 0x55, buffer.getByte(6));
    assertEquals((byte) 0x55, buffer.getByte(7 + length));
  }

  @ParameterizedTest
  @MethodSource("wireMessages")
  void decodesAnIndependentWireMessageAtANonzeroOffset(CommandRequest expected, byte[] message) {
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
  void preservesAllRequestIdBits(UUID requestId) {
    List<EngineCommand> commands = List.of(new PlaceOrder(Long.MIN_VALUE, Side.BID, Long.MAX_VALUE, Long.MAX_VALUE - 1),
        new CancelOrder(Long.MAX_VALUE));
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    for (EngineCommand command : commands) {
      CommandRequest request = new CommandRequest(requestId, command);

      int length = codec.encode(request, buffer, 3);

      assertEquals(request, codec.decode(buffer, 3, length));
    }
  }

  @Test
  void reusesDecodersAcrossRequestTypesBuffersAndOffsets() {
    ExpandableArrayBuffer first = new ExpandableArrayBuffer(128);
    first.putBytes(3, PLACE_MESSAGE);
    assertEquals(new CommandRequest(REQUEST_ID, new PlaceOrder(42L, Side.ASK, 12345L, 7L)),
        codec.decode(first, 3, PLACE_MESSAGE.length));

    ExpandableArrayBuffer second = new ExpandableArrayBuffer(128);
    second.putBytes(7, CANCEL_MESSAGE);
    assertEquals(new CommandRequest(REQUEST_ID, new CancelOrder(42L)), codec.decode(second, 7, CANCEL_MESSAGE.length));

    UUID nextId = new UUID(Long.MIN_VALUE, Long.MAX_VALUE);
    CommandRequest next = new CommandRequest(nextId, new PlaceOrder(99L, Side.BID, 101L, 2L));
    int length = codec.encode(next, second, 41);
    assertEquals(next, codec.decode(second, 41, length));

    CommandRequest cancel = new CommandRequest(nextId, new CancelOrder(99L));
    length = codec.encode(cancel, first, 17);
    assertEquals(cancel, codec.decode(first, 17, length));
  }

  @ParameterizedTest
  @MethodSource("wireMessages")
  void rejectsIncorrectFrameLengthsDespiteSpareBufferCapacity(CommandRequest request, byte[] message) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    buffer.putBytes(3, message);

    for (int length : new int[]{0, 7, 8, message.length - 1, message.length + 1}) {
      assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 3, length),
          "Template for " + request.command() + ", frame length " + length);
    }
  }

  @Test
  void rejectsPlaceRequestWithCancellationBlockLength() {
    byte[] bytes = PLACE_MESSAGE.clone();
    bytes[0] = 24;

    // The backing buffer still contains the whole place request; only 32 bytes belong to the frame.
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, 32));
  }

  @Test
  void rejectsCancelRequestWithPlaceBlockLength() {
    byte[] bytes = Arrays.copyOf(CANCEL_MESSAGE, 49);
    bytes[0] = 41;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 99})
  void rejectsUnknownTemplateIds(int templateId) {
    byte[] bytes = CANCEL_MESSAGE.clone();
    bytes[2] = (byte) templateId;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @Test
  void rejectsCommandMessagesWithoutARequestId() {
    SbeCommandCodec commandCodec = new SbeCommandCodec();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
    for (EngineCommand command : List.of(new PlaceOrder(42L, Side.ASK, 12345L, 7L), new CancelOrder(42L))) {
      int length = commandCodec.encode(command, buffer, 3);

      assertThrows(IllegalArgumentException.class, () -> codec.decode(buffer, 3, length));
    }
  }

  @ParameterizedTest
  @CsvSource({"4, 2", "6, 1"})
  void rejectsUnsupportedSchemaOrVersion(int fieldOffset, int value) {
    for (byte[] message : List.of(PLACE_MESSAGE, CANCEL_MESSAGE)) {
      byte[] bytes = message.clone();
      bytes[fieldOffset] = (byte) value;

      assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 255})
  void rejectsInvalidOrNullSideValues(int sideValue) {
    byte[] bytes = PLACE_MESSAGE.clone();
    bytes[32] = (byte) sideValue;

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }

  @ParameterizedTest
  @ValueSource(ints = {33, 41})
  void appliesDomainValidationToPriceAndQuantity(int fieldOffset) {
    byte[] bytes = PLACE_MESSAGE.clone();
    Arrays.fill(bytes, fieldOffset, fieldOffset + Long.BYTES, (byte) 0);

    assertThrows(IllegalArgumentException.class, () -> codec.decode(new UnsafeBuffer(bytes), 0, bytes.length));
  }
}
