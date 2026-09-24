package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.transport.CommandRequest;

class GrpcCommandMapperTest {
  private static final UUID REQUEST_ID = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");

  private final GrpcCommandMapper mapper = new GrpcCommandMapper();

  @ParameterizedTest
  @CsvSource({"SIDE_BID, BID", "SIDE_ASK, ASK"})
  void convertsPlaceOrdersWithoutNarrowingLongValues(dev.sam.exchange.gateway.proto.Side wireSide, Side engineSide) {
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(REQUEST_ID.toString())
        .setPlace(dev.sam.exchange.gateway.proto.PlaceOrder.newBuilder().setOrderId(9_007_199_254_740_993L)
            .setSide(wireSide).setPriceTicks(Long.MAX_VALUE).setQuantityLots(Long.MAX_VALUE - 1))
        .build();

    assertEquals(
        new CommandRequest(REQUEST_ID,
            new PlaceOrder(9_007_199_254_740_993L, engineSide, Long.MAX_VALUE, Long.MAX_VALUE - 1)),
        mapper.toCommandRequest(request));
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE})
  void preservesTheEngineOrderIdRangeForCancelCommands(long orderId) {
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(REQUEST_ID.toString())
        .setCancel(dev.sam.exchange.gateway.proto.CancelOrder.newBuilder().setOrderId(orderId)).build();

    assertEquals(new CommandRequest(REQUEST_ID, new CancelOrder(orderId)), mapper.toCommandRequest(request));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not-a-uuid", "00000000-0000-0000-0000-00000000000z", "1-1-1-1-1", "100000000-0-0-0-0"})
  void rejectsMalformedRequestIdsForBothCommands(String requestId) {
    SubmitRequest place = SubmitRequest.newBuilder().setRequestId(requestId).setPlace(validPlace()).build();
    SubmitRequest cancel = SubmitRequest.newBuilder().setRequestId(requestId)
        .setCancel(dev.sam.exchange.gateway.proto.CancelOrder.newBuilder().setOrderId(17L)).build();

    assertThrows(IllegalArgumentException.class, () -> mapper.toCommandRequest(place));
    assertThrows(IllegalArgumentException.class, () -> mapper.toCommandRequest(cancel));
  }

  @Test
  void acceptsUppercaseUuidTextWithoutChangingItsIdentity() {
    String uppercaseId = "01234567-89AB-CDEF-0123-456789ABCDEF";
    SubmitRequest place = SubmitRequest.newBuilder().setRequestId(uppercaseId).setPlace(validPlace()).build();
    SubmitRequest cancel = SubmitRequest.newBuilder().setRequestId(uppercaseId)
        .setCancel(dev.sam.exchange.gateway.proto.CancelOrder.newBuilder().setOrderId(17L)).build();

    assertEquals(REQUEST_ID, mapper.toCommandRequest(place).requestId());
    assertEquals(REQUEST_ID, mapper.toCommandRequest(cancel).requestId());
  }

  @Test
  void rejectsAnUnsetCommand() {
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(REQUEST_ID.toString()).build();

    assertThrows(IllegalArgumentException.class, () -> mapper.toCommandRequest(request));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 99})
  void rejectsUnspecifiedAndUnknownSideValues(int sideValue) {
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(REQUEST_ID.toString())
        .setPlace(validPlace().setSideValue(sideValue)).build();

    assertThrows(IllegalArgumentException.class, () -> mapper.toCommandRequest(request));
  }

  @ParameterizedTest
  @CsvSource({"0, 10", "-1, 10", "100, 0", "100, -1"})
  void appliesTheEnginePositivePriceAndQuantityRules(long priceTicks, long quantityLots) {
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(REQUEST_ID.toString())
        .setPlace(validPlace().setPriceTicks(priceTicks).setQuantityLots(quantityLots)).build();

    assertThrows(IllegalArgumentException.class, () -> mapper.toCommandRequest(request));
  }

  @Test
  void convertsPlaceResultsWithAllTradeFieldsInExecutionOrder() {
    PlaceResult result = new PlaceResult(9_007_199_254_740_993L,
        List.of(new Trade(9_007_199_254_740_993L, 12L, 100L, 4L), new Trade(9_007_199_254_740_993L, 13L, 101L, 2L)),
        7L);

    SubmitResponse response = mapper.toSubmitResponse(REQUEST_ID, result);

    assertEquals(REQUEST_ID.toString(), response.getRequestId());
    assertEquals(SubmitResponse.ResultCase.PLACE, response.getResultCase());
    assertEquals(9_007_199_254_740_993L, response.getPlace().getOrderId());
    assertEquals(7L, response.getPlace().getRemainingLots());
    assertEquals(
        List.of(
            dev.sam.exchange.gateway.proto.Trade.newBuilder().setIncomingOrderId(9_007_199_254_740_993L)
                .setRestingOrderId(12L).setPriceTicks(100L).setQuantityLots(4L).build(),
            dev.sam.exchange.gateway.proto.Trade.newBuilder().setIncomingOrderId(9_007_199_254_740_993L)
                .setRestingOrderId(13L).setPriceTicks(101L).setQuantityLots(2L).build()),
        response.getPlace().getTradesList());
  }

  @Test
  void preservesAPlaceResultEvenWhenEveryFieldHasItsDefaultValue() {
    SubmitResponse response = mapper.toSubmitResponse(REQUEST_ID, new PlaceResult(0L, List.of(), 0L));

    assertEquals(REQUEST_ID.toString(), response.getRequestId());
    assertEquals(SubmitResponse.ResultCase.PLACE, response.getResultCase());
    assertEquals(0L, response.getPlace().getOrderId());
    assertEquals(0L, response.getPlace().getRemainingLots());
    assertEquals(List.of(), response.getPlace().getTradesList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void convertsSuccessfulAndMissingOrderCancellations(boolean cancelled) {
    SubmitResponse response = mapper.toSubmitResponse(REQUEST_ID, new CancelResult(Long.MIN_VALUE, cancelled));

    assertEquals(REQUEST_ID.toString(), response.getRequestId());
    assertEquals(SubmitResponse.ResultCase.CANCEL, response.getResultCase());
    assertEquals(Long.MIN_VALUE, response.getCancel().getOrderId());
    assertEquals(cancelled, response.getCancel().getCancelled());
  }

  @ParameterizedTest
  @CsvSource({"DUPLICATE_ORDER_ID, REJECT_REASON_DUPLICATE_ORDER_ID",
      "REQUEST_ID_CONFLICT, REJECT_REASON_REQUEST_ID_CONFLICT"})
  void convertsBusinessRejectionsToTheCorrespondingWireReason(RejectReason reason,
      dev.sam.exchange.gateway.proto.RejectReason wireReason) {
    SubmitResponse response = mapper.toSubmitResponse(REQUEST_ID, new RejectResult(29L, reason));

    assertEquals(REQUEST_ID.toString(), response.getRequestId());
    assertEquals(SubmitResponse.ResultCase.REJECT, response.getResultCase());
    assertEquals(29L, response.getReject().getOrderId());
    assertEquals(wireReason, response.getReject().getReason());
  }

  private static dev.sam.exchange.gateway.proto.PlaceOrder.Builder validPlace() {
    return dev.sam.exchange.gateway.proto.PlaceOrder.newBuilder().setOrderId(17L)
        .setSide(dev.sam.exchange.gateway.proto.Side.SIDE_BID).setPriceTicks(100L).setQuantityLots(10L);
  }
}
