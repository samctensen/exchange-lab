package dev.sam.exchange.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.protocol.sbe.BooleanFlag;
import dev.sam.exchange.protocol.sbe.CancelOrderResponseDecoder;
import dev.sam.exchange.protocol.sbe.CancelOrderResponseEncoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderDecoder;
import dev.sam.exchange.protocol.sbe.MessageHeaderEncoder;
import dev.sam.exchange.protocol.sbe.PlaceOrderResponseDecoder;
import dev.sam.exchange.protocol.sbe.PlaceOrderResponseDecoder.TradesDecoder;
import dev.sam.exchange.protocol.sbe.PlaceOrderResponseEncoder;
import dev.sam.exchange.protocol.sbe.RejectOrderResponseDecoder;
import dev.sam.exchange.protocol.sbe.RejectOrderResponseEncoder;
import dev.sam.exchange.protocol.sbe.RejectionReason;
import dev.sam.exchange.transport.CommandResponse;

// Use one instance per thread. Generated codecs are reusable views into the supplied buffer.
public class SbeResponseCodec {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final PlaceOrderResponseEncoder placeEncoder = new PlaceOrderResponseEncoder();
  private final CancelOrderResponseEncoder cancelEncoder = new CancelOrderResponseEncoder();
  private final RejectOrderResponseEncoder rejectEncoder = new RejectOrderResponseEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PlaceOrderResponseDecoder placeDecoder = new PlaceOrderResponseDecoder();
  private final CancelOrderResponseDecoder cancelDecoder = new CancelOrderResponseDecoder();
  private final RejectOrderResponseDecoder rejectDecoder = new RejectOrderResponseDecoder();

  public int encode(CommandResponse response, MutableDirectBuffer buffer, int offset) {
    long mostBits = response.requestId().getMostSignificantBits();
    long leastBits = response.requestId().getLeastSignificantBits();

    return switch (response.result()) {
      case PlaceResult place -> {
        placeEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).requestIdMostSignificantBits(mostBits)
            .requestIdLeastSignificantBits(leastBits).orderId(place.orderId()).remainingLots(place.remainingLots());
        var tradesEncoder = placeEncoder.tradesCount(place.trades().size());
        for (Trade trade : place.trades()) {
          tradesEncoder.next().incomingOrderId(trade.incomingOrderId()).restingOrderId(trade.restingOrderId())
              .priceTicks(trade.priceTicks()).quantityLots(trade.quantityLots());
        }
        yield MessageHeaderEncoder.ENCODED_LENGTH + placeEncoder.encodedLength();
      }
      case CancelResult cancel -> {
        cancelEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).requestIdMostSignificantBits(mostBits)
            .requestIdLeastSignificantBits(leastBits).orderId(cancel.orderId())
            .cancelled(cancel.cancelled() ? BooleanFlag.TRUE : BooleanFlag.FALSE);
        yield MessageHeaderEncoder.ENCODED_LENGTH + cancelEncoder.encodedLength();
      }
      case RejectResult reject -> {
        rejectEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder).requestIdMostSignificantBits(mostBits)
            .requestIdLeastSignificantBits(leastBits).orderId(reject.orderId()).reason(switch (reject.reason()) {
              case DUPLICATE_ORDER_ID -> RejectionReason.DUPLICATE_ORDER_ID;
              case REQUEST_ID_CONFLICT -> RejectionReason.REQUEST_ID_CONFLICT;
            });
        yield MessageHeaderEncoder.ENCODED_LENGTH + rejectEncoder.encodedLength();
      }
    };
  }

  public CommandResponse decode(DirectBuffer buffer, int offset, int length) {
    if (offset < 0 || length < MessageHeaderDecoder.ENCODED_LENGTH || offset > buffer.capacity() - length) {
      throw new IllegalArgumentException("Invalid SBE message bounds");
    }

    headerDecoder.wrap(buffer, offset);
    verifySchemaId(headerDecoder);
    verifyVersion(headerDecoder);
    verifyBlockLength(headerDecoder);

    // Placement responses continue past the fixed fields with a trade group.
    if (headerDecoder.templateId() != PlaceOrderResponseDecoder.TEMPLATE_ID
        && length != MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
      throw new IllegalArgumentException("Frame length does not match its header");
    }

    return switch (headerDecoder.templateId()) {
      case PlaceOrderResponseDecoder.TEMPLATE_ID -> decodePlaceResponse(buffer, offset, length);
      case CancelOrderResponseDecoder.TEMPLATE_ID -> {
        cancelDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());
        UUID requestId = new UUID(cancelDecoder.requestIdMostSignificantBits(),
            cancelDecoder.requestIdLeastSignificantBits());
        boolean cancelled = switch (cancelDecoder.cancelled()) {
          case TRUE -> true;
          case FALSE -> false;
          case NULL_VAL -> throw new IllegalArgumentException("Cancellation flag is required");
        };
        CancelResult response = new CancelResult(cancelDecoder.orderId(), cancelled);
        yield new CommandResponse(requestId, response);
      }
      case RejectOrderResponseDecoder.TEMPLATE_ID -> {
        rejectDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
            headerDecoder.version());
        UUID requestId = new UUID(rejectDecoder.requestIdMostSignificantBits(),
            rejectDecoder.requestIdLeastSignificantBits());
        RejectReason reason = switch (rejectDecoder.reason()) {
          case DUPLICATE_ORDER_ID -> RejectReason.DUPLICATE_ORDER_ID;
          case REQUEST_ID_CONFLICT -> RejectReason.REQUEST_ID_CONFLICT;
          case NULL_VAL -> throw new IllegalArgumentException("Rejection reason is required");
        };
        yield new CommandResponse(requestId, new RejectResult(rejectDecoder.orderId(), reason));
      }
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
  }

  private CommandResponse decodePlaceResponse(DirectBuffer buffer, int offset, int length) {
    int minimumLength = MessageHeaderDecoder.ENCODED_LENGTH + PlaceOrderResponseDecoder.BLOCK_LENGTH
        + TradesDecoder.sbeHeaderSize();
    // trades() reads the group header, so first ensure those bytes belong to this frame.
    if (length < minimumLength) {
      throw new IllegalArgumentException("Frame is too short for a placement response");
    }
    placeDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(),
        headerDecoder.version());
    var tradesDecoder = placeDecoder.trades();
    if (tradesDecoder.actingBlockLength() != TradesDecoder.sbeBlockLength()) {
      throw new IllegalArgumentException("Invalid trade block length");
    }
    if (tradesDecoder.count() > TradesDecoder.countMaxValue()) {
      throw new IllegalArgumentException("Trades count exceeds maximum value");
    }
    long expectedLength = minimumLength + (long) tradesDecoder.count() * tradesDecoder.actingBlockLength();
    if (length != expectedLength) {
      throw new IllegalArgumentException("Frame length does not match its trade count");
    }

    UUID requestId = new UUID(placeDecoder.requestIdMostSignificantBits(),
        placeDecoder.requestIdLeastSignificantBits());
    long orderId = placeDecoder.orderId();
    long remainingLots = placeDecoder.remainingLots();
    if (remainingLots < 0) {
      throw new IllegalArgumentException("Remaining lots must not be negative");
    }
    List<Trade> trades = new ArrayList<>(tradesDecoder.count());
    for (var trade : tradesDecoder) {
      long priceTicks = trade.priceTicks();
      long quantityLots = trade.quantityLots();
      if (priceTicks <= 0 || quantityLots <= 0) {
        throw new IllegalArgumentException("Trade price and quantity must be positive");
      }
      trades.add(new Trade(trade.incomingOrderId(), trade.restingOrderId(), priceTicks, quantityLots));
    }
    return new CommandResponse(requestId, new PlaceResult(orderId, trades, remainingLots));
  }

  private void verifySchemaId(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.schemaId() != PlaceOrderResponseDecoder.SCHEMA_ID) {
      throw new IllegalArgumentException("Invalid schema ID");
    }
  }

  private void verifyVersion(MessageHeaderDecoder headerDecoder) {
    if (headerDecoder.version() != PlaceOrderResponseDecoder.SCHEMA_VERSION) {
      throw new IllegalArgumentException("Invalid version");
    }
  }

  private void verifyBlockLength(MessageHeaderDecoder header) {
    int expected = switch (header.templateId()) {
      case PlaceOrderResponseDecoder.TEMPLATE_ID -> PlaceOrderResponseDecoder.BLOCK_LENGTH;
      case CancelOrderResponseDecoder.TEMPLATE_ID -> CancelOrderResponseDecoder.BLOCK_LENGTH;
      case RejectOrderResponseDecoder.TEMPLATE_ID -> RejectOrderResponseDecoder.BLOCK_LENGTH;
      default -> throw new IllegalArgumentException("Invalid template ID");
    };
    if (header.blockLength() != expected) {
      throw new IllegalArgumentException("Invalid block length");
    }
  }
}
