package dev.sam.exchange.protocol;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.protocol.sbe.CancelOrderDecoder;
import dev.sam.exchange.protocol.sbe.CancelOrderEncoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderDecoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderEncoder;
import dev.sam.exchange.protocol.sbe.OrderSide;
import dev.sam.exchange.protocol.sbe.PlaceOrderDecoder;
import dev.sam.exchange.protocol.sbe.PlaceOrderEncoder;

// Use one instance per thread. Generated codecs are reusable views into the supplied buffer.
public class SbeCommandCodec {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PlaceOrderEncoder placeEncoder = new PlaceOrderEncoder();
  private final CancelOrderEncoder cancelEncoder = new CancelOrderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PlaceOrderDecoder placeDecoder = new PlaceOrderDecoder();
  private final CancelOrderDecoder cancelDecoder = new CancelOrderDecoder();

  public int encode(EngineCommand command, MutableDirectBuffer buffer, int offset) {
    // Write the standard SBE header, then wrap the encoder around the body immediately after it.
    return switch (command) {
      case PlaceOrder order -> {
        placeEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).orderId(order.orderId())
            .side(switch (order.side()) {
              case BID -> OrderSide.BID;
              case ASK -> OrderSide.ASK;
            }).priceTicks(order.priceTicks()).quantityLots(order.quantityLots());
        yield MessageHeaderEncoder.ENCODED_LENGTH + placeEncoder.encodedLength();
      }
      case CancelOrder order -> {
        cancelEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).orderId(order.orderId());
        yield MessageHeaderEncoder.ENCODED_LENGTH + cancelEncoder.encodedLength();
      }
    };
  }

  public EngineCommand decode(DirectBuffer buffer, int offset, int length) {
    // A transport frame can be shorter than its backing buffer. Check the supplied length first.
    if (offset < 0 || length < MessageHeaderDecoder.ENCODED_LENGTH || offset > buffer.capacity() - length) {
      throw new IllegalArgumentException("Invalid SBE message bounds");
    }
    headerDecoder.wrap(buffer, offset);
    verifySchemaIds(headerDecoder);
    verifyVersion(headerDecoder);
    verifyBlockLength(headerDecoder);

    if (length != MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
      throw new IllegalArgumentException("Frame length does not match its header");
    }

    return switch (headerDecoder.templateId()) {
      case PlaceOrderDecoder.TEMPLATE_ID -> {
        placeDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());
        Side side = switch (placeDecoder.side()) {
          case BID -> Side.BID;
          case ASK -> Side.ASK;
          case NULL_VAL -> throw new IllegalArgumentException("PlaceOrder side is required");
        };

        yield new PlaceOrder(placeDecoder.orderId(), side, placeDecoder.priceTicks(), placeDecoder.quantityLots());
      }
      case CancelOrderDecoder.TEMPLATE_ID -> {
        cancelDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());

        yield new CancelOrder(cancelDecoder.orderId());
      }
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
  }

  private void verifySchemaIds(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.schemaId() != PlaceOrderDecoder.SCHEMA_ID
        && headerDecoder.schemaId() != CancelOrderDecoder.SCHEMA_ID) {
      throw new IllegalArgumentException("Invalid schema ID");
    }
  }

  private void verifyVersion(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.version() != PlaceOrderDecoder.SCHEMA_VERSION
        && headerDecoder.version() != CancelOrderDecoder.SCHEMA_VERSION) {
      throw new IllegalArgumentException("Invalid version");
    }
  }

  private void verifyBlockLength(MessageHeaderDecoder header) {
    int expected = switch (header.templateId()) {
      case PlaceOrderDecoder.TEMPLATE_ID -> PlaceOrderDecoder.BLOCK_LENGTH;
      case CancelOrderDecoder.TEMPLATE_ID -> CancelOrderDecoder.BLOCK_LENGTH;
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
    if (header.blockLength() != expected) {
      throw new IllegalArgumentException("Invalid block length");
    }
  }
}
