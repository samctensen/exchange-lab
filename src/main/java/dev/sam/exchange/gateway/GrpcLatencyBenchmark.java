package dev.sam.exchange.gateway;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import dev.sam.exchange.gateway.proto.CancelOrder;
import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;

/** Closed-loop diagnostic: one application RPC per fresh UUID, with bounded outstanding calls. */
public class GrpcLatencyBenchmark {
  record Measurement(long[] terminalLatencies, long[] successfulLatencies, Map<Status.Code, Integer> failures,
      long elapsedNanos, int peakOutstanding) {
  }

  private record Completion(String requestId, SubmitResponse response, Status.Code failure, long elapsedNanos) {
  }

  public static void main(String[] args) throws InterruptedException {
    if (args.length > 5) {
      throw new IllegalArgumentException(
          "Usage: GrpcLatencyBenchmark [warmupCount] [sampleCount] [maxInFlight] [deadlineMillis] [port]");
    }
    int warmup = args.length > 0 ? Integer.parseInt(args[0]) : 500;
    int samples = args.length > 1 ? Integer.parseInt(args[1]) : 2000;
    int window = args.length > 2 ? Integer.parseInt(args[2]) : 8;
    int deadline = args.length > 3 ? Integer.parseInt(args[3]) : 5000;
    int port = args.length > 4 ? Integer.parseInt(args[4]) : 50051;
    if (warmup < 0 || warmup > 1_000_000 || samples < 1 || samples > 1_000_000 || window < 1 || deadline < 1 || port < 1
        || port > 65535) {
      throw new IllegalArgumentException("Counts must be at most 1000000; samples, window, deadline and port must be "
          + "positive; warmup may be zero; port must be at most 65535");
    }
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().disableRetry().build();
    Measurement measured;
    try {
      var stub = ExchangeServiceGrpc.newFutureStub(channel);
      Measurement warmed = measure(stub, warmup, window, deadline);
      if (!warmed.failures().isEmpty()) {
        throw new IllegalStateException("Warmup RPCs failed: " + warmed.failures());
      }
      measured = measure(stub, samples, window, deadline);
    } finally {
      channel.shutdownNow();
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Benchmark channel did not stop");
      }
    }
    System.out.println("Warmup: " + warmup + " requests");
    System.out.println("Max in flight: " + window);
    System.out.println("RPC deadline: " + deadline + " ms");
    System.out.println("Application RPCs per request: 1; channel retries disabled");
    System.out.print(summarize(measured));
  }

  static Measurement measure(ExchangeServiceGrpc.ExchangeServiceFutureStub stub, int count, int maxInFlight,
      long deadlineMillis) throws InterruptedException {
    if (count < 0 || count > 1_000_000 || maxInFlight < 1 || deadlineMillis < 1) {
      throw new IllegalArgumentException("Invalid benchmark counts, window or deadline");
    }
    long[] terminal = new long[count];
    long[] successful = new long[count];
    Map<Status.Code, Integer> failures = new EnumMap<>(Status.Code.class);
    // At most one completion exists per outstanding call, including calls cancelled on shutdown.
    var completions = new ArrayBlockingQueue<Completion>(Math.max(1, Math.min(count, maxInFlight)));
    int submitted = 0;
    int completed = 0;
    int successes = 0;
    int peakOutstanding = 0;
    long phaseStart = System.nanoTime();
    while (completed < count) {
      while (submitted < count && submitted - completed < maxInFlight) {
        String id = UUID.randomUUID().toString();
        SubmitRequest request = SubmitRequest.newBuilder().setRequestId(id)
            .setCancel(CancelOrder.newBuilder().setOrderId(1)).build();
        long started = System.nanoTime();
        var future = stub.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS).submit(request);
        future.addListener(() -> {
          long elapsed = System.nanoTime() - started;
          SubmitResponse response = null;
          Status.Code failure = null;
          try {
            response = future.get();
          } catch (ExecutionException e) {
            failure = Status.fromThrowable(e.getCause()).getCode();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = Status.Code.CANCELLED;
          } catch (java.util.concurrent.CancellationException e) {
            failure = Status.Code.CANCELLED;
          }
          completions.add(new Completion(id, response, failure, elapsed));
        }, Runnable::run);
        submitted++;
        peakOutstanding = Math.max(peakOutstanding, submitted - completed);
      }
      // Consume whichever RPC finished first, so a slow earlier RPC cannot hold up refilling other slots.
      Completion completion = completions.take();
      terminal[completed++] = completion.elapsedNanos();
      if (completion.failure() != null) {
        failures.merge(completion.failure(), 1, Integer::sum);
      } else {
        SubmitResponse response = completion.response();
        if (!response.getRequestId().equals(completion.requestId()) || !response.hasCancel()
            || response.getCancel().getOrderId() != 1 || response.getCancel().getCancelled()) {
          throw new IllegalStateException("Unexpected benchmark result; use a fresh, empty Archive: " + response);
        }
        successful[successes++] = completion.elapsedNanos();
      }
    }
    return new Measurement(terminal, Arrays.copyOf(successful, successes), Map.copyOf(failures),
        System.nanoTime() - phaseStart, peakOutstanding);
  }

  static String summarize(Measurement measured) {
    if (measured.elapsedNanos() <= 0 || measured.terminalLatencies().length == 0) {
      throw new IllegalArgumentException("At least one completion and positive elapsed time are required");
    }
    int errors = measured.failures().values().stream().mapToInt(Integer::intValue).sum();
    StringBuilder report = new StringBuilder(String.format(Locale.ROOT,
        "Completed: %d requests%nSuccessful: %d requests%nRPC failures: %d%nPeak outstanding: %d%n"
            + "Completion throughput: %.3f requests/s%nSuccessful throughput: %.3f requests/s%n",
        measured.terminalLatencies().length, measured.successfulLatencies().length, errors, measured.peakOutstanding(),
        measured.terminalLatencies().length * 1e9 / measured.elapsedNanos(),
        measured.successfulLatencies().length * 1e9 / measured.elapsedNanos()));
    for (Status.Code code : Status.Code.values()) {
      if (code != Status.Code.OK) {
        report.append("Status ").append(code).append(": ").append(measured.failures().getOrDefault(code, 0))
            .append(System.lineSeparator());
      }
    }
    report.append(latencies("Terminal", measured.terminalLatencies()));
    report.append(latencies("Success", measured.successfulLatencies()));
    return report.toString();
  }

  private static String latencies(String label, long[] samples) {
    if (samples.length == 0)
      return label + " latency: unavailable" + System.lineSeparator();
    long[] sorted = samples.clone();
    Arrays.sort(sorted);
    int p50 = (int) ((sorted.length * 50L + 99) / 100) - 1;
    int p99 = (int) ((sorted.length * 99L + 99) / 100) - 1;
    return String.format(Locale.ROOT, "%s p50: %.3f us%n%s p99: %.3f us%n%s max: %.3f us%n", label,
        sorted[p50] / 1000.0, label, sorted[p99] / 1000.0, label, sorted[sorted.length - 1] / 1000.0);
  }
}
