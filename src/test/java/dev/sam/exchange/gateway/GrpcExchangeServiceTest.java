package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.gateway.proto.CancelOrder;
import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.PlaceOrder;
import dev.sam.exchange.gateway.proto.RejectReason;
import dev.sam.exchange.gateway.proto.Side;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.RequestStateMachine;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

@Timeout(15)
class GrpcExchangeServiceTest {
  @Test
  void returnsCorrelatedPlaceTradeAndCancelResultsOverGrpc() throws Exception {
    try (Fixture fixture = new Fixture()) {
      SubmitResponse bid = fixture.submit(place(1, 101, Side.SIDE_BID, 100, 10));
      assertEquals(id(1), bid.getRequestId());
      assertEquals(SubmitResponse.ResultCase.PLACE, bid.getResultCase());
      assertEquals(101L, bid.getPlace().getOrderId());
      assertEquals(10L, bid.getPlace().getRemainingLots());
      assertEquals(0, bid.getPlace().getTradesCount());

      SubmitResponse ask = fixture.submit(place(2, 102, Side.SIDE_ASK, 99, 4));
      assertEquals(id(2), ask.getRequestId());
      assertEquals(SubmitResponse.ResultCase.PLACE, ask.getResultCase());
      assertEquals(102L, ask.getPlace().getOrderId());
      assertEquals(0L, ask.getPlace().getRemainingLots());
      assertEquals(1, ask.getPlace().getTradesCount());
      var trade = ask.getPlace().getTrades(0);
      assertEquals(102L, trade.getIncomingOrderId());
      assertEquals(101L, trade.getRestingOrderId());
      assertEquals(100L, trade.getPriceTicks());
      assertEquals(4L, trade.getQuantityLots());

      SubmitResponse cancelled = fixture.submit(cancel(3, 101));
      assertEquals(id(3), cancelled.getRequestId());
      assertEquals(SubmitResponse.ResultCase.CANCEL, cancelled.getResultCase());
      assertEquals(101L, cancelled.getCancel().getOrderId());
      assertTrue(cancelled.getCancel().getCancelled());
      assertFalse(fixture.submit(cancel(4, 101)).getCancel().getCancelled());
    }
  }

  @Test
  void retriesKeepTheOriginalUuidAndDoNotExecuteAnOrderTwice() throws Exception {
    try (Fixture fixture = new Fixture()) {
      SubmitRequest bid = place(1, 101, Side.SIDE_BID, 100, 10);
      SubmitResponse original = fixture.submit(bid);
      SubmitRequest ask = place(2, 102, Side.SIDE_ASK, 99, 4);
      SubmitResponse filled = fixture.submit(ask);

      assertEquals(filled, fixture.submit(ask));
      assertEquals(original, fixture.submit(bid));

      // A repeated ask must not consume another four lots from the resting bid.
      SubmitResponse remainingFill = fixture.submit(place(3, 103, Side.SIDE_ASK, 99, 10));
      assertEquals(1, remainingFill.getPlace().getTradesCount());
      assertEquals(6L, remainingFill.getPlace().getTrades(0).getQuantityLots());
      assertEquals(4L, remainingFill.getPlace().getRemainingLots());
    }
  }

  @Test
  void returnsBusinessRejectionsAsNormalResponses() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.submit(place(1, 101, Side.SIDE_BID, 100, 10));

      SubmitResponse duplicate = fixture.submit(place(2, 101, Side.SIDE_BID, 100, 10));
      assertEquals(id(2), duplicate.getRequestId());
      assertEquals(SubmitResponse.ResultCase.REJECT, duplicate.getResultCase());
      assertEquals(101L, duplicate.getReject().getOrderId());
      assertEquals(RejectReason.REJECT_REASON_DUPLICATE_ORDER_ID, duplicate.getReject().getReason());

      SubmitResponse conflict = fixture.submit(cancel(1, 101));
      assertEquals(id(1), conflict.getRequestId());
      assertEquals(SubmitResponse.ResultCase.REJECT, conflict.getResultCase());
      assertEquals(RejectReason.REJECT_REASON_REQUEST_ID_CONFLICT, conflict.getReject().getReason());
      assertTrue(fixture.submit(cancel(3, 101)).getCancel().getCancelled());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void invalidInputReturnsInvalidArgumentWithoutReachingTheEngine(boolean invalidUuid) throws Exception {
    AtomicInteger sends = new AtomicInteger();
    try (Fixture fixture = new Fixture(request -> {
      sends.incrementAndGet();
      throw new IllegalStateException("Invalid input reached transport");
    }, new GrpcCommandMapper())) {
      SubmitRequest request = invalidUuid
          ? cancel(1, 101).toBuilder().setRequestId("not-a-uuid").build()
          : SubmitRequest.newBuilder().setRequestId(id(1)).build();

      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> fixture.submit(request));

      assertEquals(Status.Code.INVALID_ARGUMENT, failure.getStatus().getCode());
      assertEquals(0, sends.get());
    }
  }

  @Test
  void transportFailureReturnsUnavailableWithSafeRetryInstructions() throws Exception {
    try (Fixture fixture = new Fixture(request -> {
      throw new IllegalStateException("No Aeron reply; outcome unknown");
    }, new GrpcCommandMapper())) {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> fixture.submit(cancel(1, 101)));

      assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
      assertTrue(failure.getStatus().getDescription().contains(id(1)));
      assertTrue(failure.getStatus().getDescription().contains("same request ID and command"));
    }
  }

  @Test
  void aClosedGatewayReturnsUnavailableInsteadOfLeavingTheRpcOpen() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.gateway.close();

      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> fixture.submit(cancel(1, 101)));

      assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    }
  }

  @Test
  void aResponseMappingFailureTerminatesTheRpcWithInternal() throws Exception {
    GrpcCommandMapper brokenMapper = new GrpcCommandMapper() {
      @Override
      public SubmitResponse toSubmitResponse(UUID requestId, CommandResult result) {
        throw new IllegalStateException("Response conversion failed");
      }
    };
    try (Fixture fixture = new Fixture(engine(), brokenMapper)) {
      StatusRuntimeException failure = assertThrows(StatusRuntimeException.class, () -> fixture.submit(cancel(1, 101)));

      assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
    }
  }

  private static String id(long value) {
    return new UUID(0L, value).toString();
  }

  private static SubmitRequest place(long requestId, long orderId, Side side, long price, long quantity) {
    return SubmitRequest.newBuilder().setRequestId(id(requestId))
        .setPlace(
            PlaceOrder.newBuilder().setOrderId(orderId).setSide(side).setPriceTicks(price).setQuantityLots(quantity))
        .build();
  }

  private static SubmitRequest cancel(long requestId, long orderId) {
    return SubmitRequest.newBuilder().setRequestId(id(requestId))
        .setCancel(CancelOrder.newBuilder().setOrderId(orderId)).build();
  }

  private static Function<CommandRequest, CommandResult> engine() {
    RequestStateMachine stateMachine = new RequestStateMachine(new MatchingEngine(new OrderBook()));
    return request -> stateMachine.process(request).result();
  }

  // Exercise real TCP/protobuf, the adapter, and the gateway worker. Replace only Aeron I/O.
  private static final class Fixture implements AutoCloseable {
    private final EngineGateway gateway;
    private final Server server;
    private final ManagedChannel channel;

    private Fixture() throws Exception {
      this(engine(), new GrpcCommandMapper());
    }

    private Fixture(Function<CommandRequest, CommandResult> send, GrpcCommandMapper mapper) throws Exception {
      AeronRequestClient client = new AeronRequestClient(null, null) {
        @Override
        public CommandResult send(CommandRequest request) {
          return send.apply(request);
        }
      };
      gateway = new EngineGateway(client, 8);
      server = ServerBuilder.forPort(0).addService(new GrpcExchangeService(gateway, mapper)).build();
      gateway.start();
      try {
        server.start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
      } catch (Exception | Error failure) {
        server.shutdownNow();
        gateway.close();
        throw failure;
      }
    }

    private SubmitResponse submit(SubmitRequest request) {
      return ExchangeServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS).submit(request);
    }

    @Override
    public void close() throws InterruptedException {
      channel.shutdownNow();
      server.shutdownNow();
      try {
        gateway.close();
      } finally {
        assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS), "Client channel did not stop");
        assertTrue(server.awaitTermination(3, TimeUnit.SECONDS), "gRPC server did not stop");
      }
    }
  }
}
