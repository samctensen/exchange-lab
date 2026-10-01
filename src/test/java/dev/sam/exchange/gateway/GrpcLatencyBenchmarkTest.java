package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

@Timeout(10)
class GrpcLatencyBenchmarkTest {
  @Test
  void fillsTheWindowHandlesReverseRepliesAndUsesFreshIdsAcrossPhases() throws Exception {
    List<SubmitRequest> seen = new ArrayList<>();
    List<StreamObserver<SubmitResponse>> pending = new ArrayList<>();
    var service = new ExchangeServiceGrpc.ExchangeServiceImplBase() {
      @Override
      public synchronized void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
        seen.add(request);
        pending.add(observer);
        // A serial benchmark cannot get the second reply; completion order differs from send order.
        if (pending.size() == 2) {
          int base = seen.size() - 2;
          for (int index = 1; index >= 0; index--) {
            pending.get(index).onNext(response(seen.get(base + index), false));
            pending.get(index).onCompleted();
          }
          pending.clear();
        }
      }
    };
    try (Peer peer = new Peer(service)) {
      var warmup = GrpcLatencyBenchmark.measure(peer.stub(), 2, 2, 2000);
      var measured = GrpcLatencyBenchmark.measure(peer.stub(), 6, 2, 2000);
      assertEquals(2, warmup.successfulLatencies().length);
      assertEquals(6, measured.successfulLatencies().length);
      assertEquals(6, measured.terminalLatencies().length);
      assertTrue(measured.failures().isEmpty());
      assertEquals(2, measured.peakOutstanding());
      assertEquals(8, new HashSet<>(seen.stream().map(SubmitRequest::getRequestId).toList()).size());
      assertTrue(seen.stream().allMatch(r -> r.hasCancel() && r.getCancel().getOrderId() == 1));
    }
  }

  @Test
  void countsRpcErrorsAndDeadlinesWithoutIncludingThemInSuccessLatency() throws Exception {
    var service = new ExchangeServiceGrpc.ExchangeServiceImplBase() {
      int calls;
      @Override
      public synchronized void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
        switch (++calls) {
          case 1 -> observer.onError(Status.UNAVAILABLE.asRuntimeException());
          case 2 -> {
          } // Client deadline ends this call; it must free a slot.
          default -> {
            observer.onNext(response(request, false));
            observer.onCompleted();
          }
        }
      }
    };
    try (Peer peer = new Peer(service)) {
      // Connect outside the short request deadline.
      peer.channel.getState(true);
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (peer.channel.getState(false) != io.grpc.ConnectivityState.READY) {
        assertTrue(System.nanoTime() - until < 0);
        Thread.sleep(5);
      }
      var measured = GrpcLatencyBenchmark.measure(peer.stub(), 3, 1, 100);
      assertEquals(3, measured.terminalLatencies().length);
      assertEquals(1, measured.successfulLatencies().length);
      assertEquals(Map.of(Status.Code.UNAVAILABLE, 1, Status.Code.DEADLINE_EXCEEDED, 1), measured.failures());
      String report = GrpcLatencyBenchmark.summarize(measured);
      assertTrue(report.contains("RPC failures: 2"), report);
      assertTrue(report.contains("Status DEADLINE_EXCEEDED: 1"), report);
      assertTrue(report.contains("Successful: 1 requests"), report);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsUnexpectedBusinessResultsOrRequestIds(boolean wrongId) throws Exception {
    var service = new ExchangeServiceGrpc.ExchangeServiceImplBase() {
      @Override
      public void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
        observer.onNext(
            wrongId ? response(request, false).toBuilder().setRequestId("wrong-id").build() : response(request, true));
        observer.onCompleted();
      }
    };
    try (Peer peer = new Peer(service)) {
      assertThrows(IllegalStateException.class, () -> GrpcLatencyBenchmark.measure(peer.stub(), 1, 1, 2000));
    }
  }

  @Test
  void computesNearestRankLatencyAndSuccessThroughputSeparatelyFromCompletions() {
    var measured = new GrpcLatencyBenchmark.Measurement(new long[]{1000, 2000, 9000, 10000}, new long[]{1000, 2000},
        Map.of(Status.Code.UNAVAILABLE, 2), 1_000_000_000L, 2);
    String report = GrpcLatencyBenchmark.summarize(measured);
    assertTrue(report.contains("Terminal p50: 2.000 us"), report);
    assertTrue(report.contains("Terminal p99: 10.000 us"), report);
    assertTrue(report.contains("Success p99: 2.000 us"), report);
    assertTrue(report.contains("Successful throughput: 2.000 requests/s"), report);
    assertTrue(report.contains("Completion throughput: 4.000 requests/s"), report);
  }

  @Test
  void reportsAllFailedCallsWithoutInventingSuccessfulLatency() {
    var measured = new GrpcLatencyBenchmark.Measurement(new long[]{1000}, new long[0],
        Map.of(Status.Code.UNAVAILABLE, 1), 1_000_000_000L, 1);
    String report = GrpcLatencyBenchmark.summarize(measured);
    assertTrue(report.contains("Success latency: unavailable"), report);
    assertTrue(report.contains("Successful throughput: 0.000 requests/s"), report);
  }

  @ParameterizedTest
  @CsvSource({"-1,1,1,5000,50051", "0,0,1,5000,50051", "0,1000001,1,5000,50051", "0,1,0,5000,50051", "0,1,1,0,50051",
      "0,1,1,5000,0", "0,1,1,5000,65536"})
  void rejectsInvalidArgumentsBeforeConnecting(String warmup, String samples, String window, String deadline,
      String port) {
    assertThrows(IllegalArgumentException.class,
        () -> GrpcLatencyBenchmark.main(new String[]{warmup, samples, window, deadline, port}));
  }

  private static SubmitResponse response(SubmitRequest request, boolean cancelled) {
    return SubmitResponse.newBuilder().setRequestId(request.getRequestId())
        .setCancel(CancelResult.newBuilder().setOrderId(1).setCancelled(cancelled)).build();
  }

  private static final class Peer implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    Peer(ExchangeServiceGrpc.ExchangeServiceImplBase service) throws Exception {
      server = ServerBuilder.forPort(0).addService(service).build().start();
      channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().disableRetry().build();
    }
    ExchangeServiceGrpc.ExchangeServiceFutureStub stub() {
      return ExchangeServiceGrpc.newFutureStub(channel);
    }
    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
      assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS));
      assertTrue(server.awaitTermination(3, TimeUnit.SECONDS));
    }
  }
}
