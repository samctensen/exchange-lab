package dev.sam.exchange.gateway;

import java.util.UUID;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.gateway.proto.RejectReason;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.transport.CommandRequest;

public class GrpcCommandMapper {
  public CommandRequest toCommandRequest(SubmitRequest request) {
    UUID requestId = UUID.fromString(request.getRequestId());
    // UUID.fromString also accepts shortened groups; require the complete wire identity.
    if (!requestId.toString().equalsIgnoreCase(request.getRequestId())) {
      throw new IllegalArgumentException("request_id must use the full 8-4-4-4-12 UUID format");
    }
    return switch (request.getCommandCase()) {
      case PLACE -> new CommandRequest(requestId, grpcToPlaceOrder(request.getPlace()));
      case CANCEL -> new CommandRequest(requestId, grpcToCancelOrder(request.getCancel()));
      default -> throw new IllegalArgumentException("Unknown command case: " + request.getCommandCase());
    };
  }

  public SubmitResponse toSubmitResponse(UUID requestId, CommandResult result) {
    var response = SubmitResponse.newBuilder().setRequestId(requestId.toString());

    switch (result) {
      case PlaceResult place -> {
        var placed = dev.sam.exchange.gateway.proto.PlaceResult.newBuilder().setOrderId(place.orderId())
            .setRemainingLots(place.remainingLots());

        for (Trade trade : place.trades()) {
          placed.addTrades(dev.sam.exchange.gateway.proto.Trade.newBuilder().setIncomingOrderId(trade.incomingOrderId())
              .setRestingOrderId(trade.restingOrderId()).setPriceTicks(trade.priceTicks())
              .setQuantityLots(trade.quantityLots()));
        }

        response.setPlace(placed);
      }

      case CancelResult cancel -> {
        response.setCancel(dev.sam.exchange.gateway.proto.CancelResult.newBuilder().setOrderId(cancel.orderId())
            .setCancelled(cancel.cancelled()));
      }

      case RejectResult reject -> {
        RejectReason reason = switch (reject.reason()) {
          case DUPLICATE_ORDER_ID -> RejectReason.REJECT_REASON_DUPLICATE_ORDER_ID;
          case REQUEST_ID_CONFLICT -> RejectReason.REJECT_REASON_REQUEST_ID_CONFLICT;
        };

        response.setReject(
            dev.sam.exchange.gateway.proto.RejectResult.newBuilder().setOrderId(reject.orderId()).setReason(reason));
      }
    }

    return response.build();
  }

  private PlaceOrder grpcToPlaceOrder(dev.sam.exchange.gateway.proto.PlaceOrder order) {

    Side side = switch (order.getSide()) {
      case SIDE_BID -> Side.BID;
      case SIDE_ASK -> Side.ASK;
      case SIDE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("side must be BID or ASK");
    };

    return new PlaceOrder(order.getOrderId(), side, order.getPriceTicks(), order.getQuantityLots());
  }

  private CancelOrder grpcToCancelOrder(dev.sam.exchange.gateway.proto.CancelOrder order) {
    return new CancelOrder(order.getOrderId());
  }
}
