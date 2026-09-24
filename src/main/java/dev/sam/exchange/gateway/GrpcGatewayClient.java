package dev.sam.exchange.gateway;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.PlaceOrder;
import dev.sam.exchange.gateway.proto.Side;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcGatewayClient {
  public static void main(String[] args) throws InterruptedException {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
    SubmitRequest request = SubmitRequest.newBuilder().setRequestId(UUID.randomUUID().toString())
        .setPlace(
            PlaceOrder.newBuilder().setOrderId(1001L).setSide(Side.SIDE_BID).setPriceTicks(100L).setQuantityLots(10L))
        .build();
    try {
      var stub = ExchangeServiceGrpc.newBlockingStub(channel);

      SubmitResponse response = stub.withDeadlineAfter(5, TimeUnit.SECONDS).submit(request);

      System.out.println(response);
    } finally {
      channel.shutdown();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    }
  }
}
