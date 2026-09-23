package dev.sam.exchange.protocol;

import java.util.UUID;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.protocol.sbe.CancelOrderRequestDecoder;
import dev.sam.exchange.protocol.sbe.CancelOrderRequestEncoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderDecoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderEncoder;
import dev.sam.exchange.protocol.sbe.OrderSide;
import dev.sam.exchange.protocol.sbe.PlaceOrderRequestDecoder;
import dev.sam.exchange.protocol.sbe.PlaceOrderRequestEncoder;
import dev.sam.exchange.transport.CommandRequest;

// Use one instance per thread. Generated codecs are reusable views into the supplied buffer.
public class SbeRequestCodec {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PlaceOrderRequestEncoder placeEncoder = new PlaceOrderRequestEncoder();
  private final CancelOrderRequestEncoder cancelEncoder = new CancelOrderRequestEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PlaceOrderRequestDecoder placeDecoder = new PlaceOrderRequestDecoder();
  private final CancelOrderRequestDecoder cancelDecoder = new CancelOrderRequestDecoder();

  public int encode(CommandRequest request, MutableDirectBuffer buffer, int offset) {
    long mostBits = request.requestId().getMostSignificantBits();
    long leastBits = request.requestId().getLeastSignificantBits();

    return switch (request.command()) {
      case PlaceOrder order -> {
        placeEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).requestIdMostSignificantBits(mostBits)
            .requestIdLeastSignificantBits(leastBits).orderId(order.orderId()).side(switch (order.side()) {
              case BID -> OrderSide.BID;
              case ASK -> OrderSide.ASK;
            }).priceTicks(order.priceTicks()).quantityLots(order.quantityLots());
        yield MessageHeaderEncoder.ENCODED_LENGTH + placeEncoder.encodedLength();
      }
      case CancelOrder cancel -> {
        cancelEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).requestIdMostSignificantBits(mostBits)
            .requestIdLeastSignificantBits(leastBits).orderId(cancel.orderId());

        yield MessageHeaderEncoder.ENCODED_LENGTH + cancelEncoder.encodedLength();
      }
    };
  }

  public CommandRequest decode(DirectBuffer buffer, int offset, int length) {
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
      case PlaceOrderRequestDecoder.TEMPLATE_ID -> {
        placeDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());
        UUID requestId = new UUID(placeDecoder.requestIdMostSignificantBits(),
            placeDecoder.requestIdLeastSignificantBits());
        Side side = switch (placeDecoder.side()) {
          case BID -> Side.BID;
          case ASK -> Side.ASK;
          case NULL_VAL -> throw new IllegalArgumentException("PlaceOrder side is required");
        };
        PlaceOrder command = new PlaceOrder(placeDecoder.orderId(), side, placeDecoder.priceTicks(),
            placeDecoder.quantityLots());
        yield new CommandRequest(requestId, command);
      }
      case CancelOrderRequestDecoder.TEMPLATE_ID -> {
        cancelDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());

        UUID requestId = new UUID(cancelDecoder.requestIdMostSignificantBits(),
            cancelDecoder.requestIdLeastSignificantBits());

        CancelOrder command = new CancelOrder(cancelDecoder.orderId());

        yield new CommandRequest(requestId, command);
      }
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
  }

  private void verifySchemaIds(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.schemaId() != PlaceOrderRequestDecoder.SCHEMA_ID
        && headerDecoder.schemaId() != CancelOrderRequestDecoder.SCHEMA_ID) {
      throw new IllegalArgumentException("Invalid schema ID");
    }
  }

  private void verifyVersion(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.version() != PlaceOrderRequestDecoder.SCHEMA_VERSION
        && headerDecoder.version() != CancelOrderRequestDecoder.SCHEMA_VERSION) {
      throw new IllegalArgumentException("Invalid version");
    }
  }

  private void verifyBlockLength(MessageHeaderDecoder header) {
    int expected = switch (header.templateId()) {
      case PlaceOrderRequestDecoder.TEMPLATE_ID -> PlaceOrderRequestDecoder.BLOCK_LENGTH;
      case CancelOrderRequestDecoder.TEMPLATE_ID -> CancelOrderRequestDecoder.BLOCK_LENGTH;
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
    if (header.blockLength() != expected) {
      throw new IllegalArgumentException("Invalid block length");
    }
  }
}
