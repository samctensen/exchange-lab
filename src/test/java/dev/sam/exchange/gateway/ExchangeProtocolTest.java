package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HexFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.protobuf.Message;

import dev.sam.exchange.gateway.proto.CancelOrder;
import dev.sam.exchange.gateway.proto.CancelResult;
import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.PlaceOrder;
import dev.sam.exchange.gateway.proto.PlaceResult;
import dev.sam.exchange.gateway.proto.RejectReason;
import dev.sam.exchange.gateway.proto.RejectResult;
import dev.sam.exchange.gateway.proto.Side;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.gateway.proto.Trade;
import io.grpc.MethodDescriptor;

class ExchangeProtocolTest {
  @ParameterizedTest(name = "{0}")
  @MethodSource("wireFixtures")
  void readsV1WireMessagesWithoutChangingTheirMeaning(String name, String hex, Message expected) throws Exception {
    // These independent wire fixtures catch accidental field renumbering and integer narrowing.
    // Do not require canonical byte output: protobuf permits equivalent field orderings.
    assertEquals(expected, expected.getParserForType().parseFrom(HexFormat.of().parseHex(hex)));
  }

  @Test
  void exposesTheVersionedUnarySubmitMethod() {
    MethodDescriptor<SubmitRequest, SubmitResponse> submit = ExchangeServiceGrpc.getSubmitMethod();
    assertEquals("exchange.v1.ExchangeService/Submit", submit.getFullMethodName());
    assertEquals(MethodDescriptor.MethodType.UNARY, submit.getType());
  }

  private static Stream<Arguments> wireFixtures() {
    String first = "00000000-0000-0000-0000-000000000001";
    String second = "00000000-0000-0000-0000-000000000002";
    String third = "00000000-0000-0000-0000-000000000003";
    return Stream.of(Arguments.of("bid with an order ID larger than a double can represent exactly",
        "0a2430303030303030302d303030302d303030302d303030302d303030303030303030303031120f08818080808080801010011864200a",
        SubmitRequest.newBuilder().setRequestId(first)
            .setPlace(PlaceOrder.newBuilder().setOrderId(9_007_199_254_740_993L).setSide(Side.SIDE_BID)
                .setPriceTicks(100L).setQuantityLots(10L))
            .build()),
        Arguments.of("ask preserving signed 64-bit limits",
            "0a2430303030303030302d303030302d303030302d303030302d303030303030303030303032122008ffffffffffffffff7f100218ffffffffffffffff7f20ffffffffffffffff7f",
            SubmitRequest.newBuilder().setRequestId(second)
                .setPlace(PlaceOrder.newBuilder().setOrderId(Long.MAX_VALUE).setSide(Side.SIDE_ASK)
                    .setPriceTicks(Long.MAX_VALUE).setQuantityLots(Long.MAX_VALUE))
                .build()),
        Arguments.of("cancel preserving a negative order ID supported by the engine",
            "0a2430303030303030302d303030302d303030302d303030302d3030303030303030303030331a0b0880808080808080808001",
            SubmitRequest.newBuilder().setRequestId(third)
                .setCancel(CancelOrder.newBuilder().setOrderId(Long.MIN_VALUE)).build()),
        Arguments.of("place result preserving trade order and remaining lots",
            "0a2430303030303030302d303030302d303030302d303030302d303030303030303030303031122d08818080808080801010041a0f0881808080808080101002186420041a0f088180808080808010100318632002",
            SubmitResponse.newBuilder().setRequestId(first)
                .setPlace(PlaceResult.newBuilder().setOrderId(9_007_199_254_740_993L).setRemainingLots(4L)
                    .addTrades(Trade.newBuilder().setIncomingOrderId(9_007_199_254_740_993L).setRestingOrderId(2L)
                        .setPriceTicks(100L).setQuantityLots(4L))
                    .addTrades(
                        Trade.newBuilder().setIncomingOrderId(9_007_199_254_740_993L).setRestingOrderId(3L)
                            .setPriceTicks(99L).setQuantityLots(2L)))
                .build()),
        Arguments.of("successful cancellation",
            "0a2430303030303030302d303030302d303030302d303030302d3030303030303030303030331a0d08808080808080808080011001",
            SubmitResponse.newBuilder().setRequestId(third)
                .setCancel(CancelResult.newBuilder().setOrderId(Long.MIN_VALUE).setCancelled(true)).build()),
        Arguments.of("missing order zero is still an explicit cancel result",
            "0a2430303030303030302d303030302d303030302d303030302d3030303030303030303030331a00",
            SubmitResponse.newBuilder().setRequestId(third).setCancel(CancelResult.getDefaultInstance()).build()),
        Arguments.of("duplicate order rejection",
            "0a2430303030303030302d303030302d303030302d303030302d303030303030303030303031220408011001",
            SubmitResponse.newBuilder().setRequestId(first)
                .setReject(
                    RejectResult.newBuilder().setOrderId(1L).setReason(RejectReason.REJECT_REASON_DUPLICATE_ORDER_ID))
                .build()),
        Arguments.of("request ID conflict rejection",
            "0a2430303030303030302d303030302d303030302d303030302d303030303030303030303031220408021002",
            SubmitResponse.newBuilder().setRequestId(first)
                .setReject(
                    RejectResult.newBuilder().setOrderId(2L).setReason(RejectReason.REJECT_REASON_REQUEST_ID_CONFLICT))
                .build()));
  }
}
