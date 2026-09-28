package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.gateway.proto.CancelOrder;
import dev.sam.exchange.gateway.proto.CancelResult;
import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

class GrpcGatewayClientTest {
  @Test
  @Timeout(10)
  void submitsTheWholeBatchAndReportsSuccessAfterAnRpcFailure() throws Exception {
    List<SubmitRequest> requests = List.of(request(1), request(2), request(3));
    SubmitResponse firstResponse = response(requests.get(0));
    SubmitResponse lastResponse = response(requests.get(2));
    Map<String, StreamObserver<SubmitResponse>> observers = new ConcurrentHashMap<>();
    AtomicInteger received = new AtomicInteger();
    Server server = ServerBuilder.forPort(0).addService(new ExchangeServiceGrpc.ExchangeServiceImplBase() {
      @Override
      public void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
        observers.put(request.getRequestId(), observer);
        // No reply until every request arrives: waiting inside the submission loop would stall.
        if (received.incrementAndGet() == requests.size()) {
          succeed(observers.get(requests.get(2).getRequestId()), lastResponse);
          observers.get(requests.get(1).getRequestId())
              .onError(Status.UNAVAILABLE.withDescription("Deliberate test failure").asRuntimeException());
          succeed(observers.get(requests.get(0).getRequestId()), firstResponse);
        }
      }
    }).build().start();
    try {
      ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
      try {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);
            PrintStream err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
          GrpcGatewayClient.submitBatch(ExchangeServiceGrpc.newFutureStub(channel), requests, out, err);
        }

        assertEquals(3, received.get());
        assertEquals(firstResponse + System.lineSeparator() + lastResponse + System.lineSeparator(),
            output.toString(StandardCharsets.UTF_8));
        String failure = errors.toString(StandardCharsets.UTF_8);
        assertEquals(1, failure.lines().count(), "Only the failed RPC belongs on stderr");
        assertTrue(failure.contains("Request " + requests.get(1).getRequestId() + " failed:"));
        assertTrue(failure.contains("UNAVAILABLE"));
        assertTrue(failure.contains("Deliberate test failure"));
        assertFalse(failure.contains(requests.get(0).getRequestId()));
        assertFalse(failure.contains(requests.get(2).getRequestId()));
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS), "gRPC client did not stop");
      }
    } finally {
      server.shutdownNow();
      assertTrue(server.awaitTermination(3, TimeUnit.SECONDS), "gRPC server did not stop");
    }
  }

  private static SubmitRequest request(long id) {
    return SubmitRequest.newBuilder().setRequestId(new UUID(0L, id).toString())
        .setCancel(CancelOrder.newBuilder().setOrderId(id)).build();
  }

  private static SubmitResponse response(SubmitRequest request) {
    return SubmitResponse.newBuilder().setRequestId(request.getRequestId())
        .setCancel(CancelResult.newBuilder().setOrderId(request.getCancel().getOrderId()).setCancelled(true)).build();
  }

  private static void succeed(StreamObserver<SubmitResponse> observer, SubmitResponse response) {
    observer.onNext(response);
    observer.onCompleted();
  }
}
