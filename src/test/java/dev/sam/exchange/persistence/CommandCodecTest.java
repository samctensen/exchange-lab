package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;

class CommandCodecTest {

  @Test
  void encodesPlaceOrder() {
    PlaceOrder order = new PlaceOrder(1L, Side.BID, 100L, 10L);
    CommandCodec codec = new CommandCodec();
    assertEquals("PLACE,1,BID,100,10", codec.encode(order));
  }

  @Test
  void encodesCancelOrder() {
    CancelOrder order = new CancelOrder(1L);
    CommandCodec codec = new CommandCodec();
    assertEquals("CANCEL,1", codec.encode(order));
  }

  @Test
  void decodesPlaceOrder() {
    String line = "PLACE,1,BID,100,10";
    CommandCodec codec = new CommandCodec();
    assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), codec.decode(line));
  }

  @Test
  void decodesCancelOrder() {
    String line = "CANCEL,1";
    CommandCodec codec = new CommandCodec();
    assertEquals(new CancelOrder(1L), codec.decode(line));
  }

  @Test
  void rejectsUnknownCommandType() {
    String line = "UNKNOWN,1";
    CommandCodec codec = new CommandCodec();
    assertThrows(IllegalArgumentException.class, () -> codec.decode(line));
  }

  @Test
  void rejectsMissingField() {
    String line = "PLACE,1,BID,100";
    CommandCodec codec = new CommandCodec();
    assertThrows(IllegalArgumentException.class, () -> codec.decode(line));
  }

}
