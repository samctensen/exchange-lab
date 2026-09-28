package dev.sam.exchange.gateway;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
    List<SubmitRequest> requests = new ArrayList<>();

    for (int i = 0; i < 16; i++) {
      SubmitRequest request = SubmitRequest.newBuilder().setRequestId(UUID.randomUUID().toString())
          .setPlace(
              PlaceOrder.newBuilder().setOrderId(i).setSide(Side.SIDE_BID).setPriceTicks(100L).setQuantityLots(10L))
          .build();
      requests.add(request);
    }
    try {
      var stub = ExchangeServiceGrpc.newFutureStub(channel);
      submitBatch(stub, requests, System.out, System.err);

    } finally {
      channel.shutdown();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    }
  }

  static void submitBatch(ExchangeServiceGrpc.ExchangeServiceFutureStub stub, List<SubmitRequest> requests,
      PrintStream out, PrintStream err) throws InterruptedException {
    List<Future<SubmitResponse>> pending = new ArrayList<>();
    for (SubmitRequest request : requests) {
      pending.add(stub.withDeadlineAfter(5, TimeUnit.SECONDS).submit(request));
    }

    for (int i = 0; i < pending.size(); i++) {
      Future<SubmitResponse> result = pending.get(i);
      try {
        out.println(result.get());
      } catch (ExecutionException e) {
        // One failed RPC must not hide the outcomes of requests already submitted.
        err.println("Request " + requests.get(i).getRequestId() + " failed: " + e.getCause());
      }
    }
  }
}
