package dev.sam.exchange.transport;

import java.util.HexFormat;

import org.agrona.ExpandableArrayBuffer;

import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.protocol.SbeCommandCodec;
import dev.sam.exchange.protocol.sbe.MessageHeaderDecoder;

// First SBE lesson: one domain order -> binary message -> a new domain order, all in memory.
public class SbeOrderDemo {
  public static void main(String[] args) {
    PlaceOrder original = new PlaceOrder(42L, Side.ASK, 12345L, 7L);
    SbeCommandCodec codec = new SbeCommandCodec();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);

    // The buffer has spare capacity. Only the returned number of bytes belongs to this message.
    int length = codec.encode(original, buffer, 0);
    MessageHeaderDecoder header = new MessageHeaderDecoder().wrap(buffer, 0);
    System.out.println("Original: " + original);
    System.out.println("Header: blockLength=" + header.blockLength() + ", templateId=" + header.templateId()
        + ", schemaId=" + header.schemaId() + ", version=" + header.version());
    System.out.println("Message length: " + length + " bytes (8-byte header + 25-byte body)");

    // Hex is only for displaying the binary bytes. The codec never converts fields through text.
    byte[] encoded = new byte[length];
    buffer.getBytes(0, encoded);
    System.out.println("Hex: " + HexFormat.of().formatHex(encoded));

    var decoded = codec.decode(buffer, 0, length);
    System.out.println("Decoded: " + decoded);
    System.out.println("Equal: " + original.equals(decoded));
  }
}
