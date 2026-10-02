package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import io.aeron.Publication;

@Timeout(10)
class EngineGatewayTest {
  @Test
  void rejectsRequestsBeforeStartup() {
    EngineGateway gateway = new EngineGateway(client(EngineGatewayTest::cancelResult), 1);

    CompletableFuture<CommandResult> result = gateway.submit(new CommandRequest(new UUID(0L, 1L), new CancelOrder(1L)));

    assertTrue(result.isCompletedExceptionally(), "An unstarted gateway must reject instead of queueing work");
    CompletionException failure = assertThrows(CompletionException.class, result::join);
    assertInstanceOf(RejectedExecutionException.class, failure.getCause());
  }

  @Test
  void startsOnceAndReturnsResultsToTheCorrectCallers() throws Exception {
    EngineGateway gateway = new EngineGateway(client(EngineGatewayTest::cancelResult), 2);
    gateway.start();
    try {
      assertThrows(IllegalStateException.class, gateway::start);
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      CompletableFuture<CommandResult> second = gateway.submit(request(2));
      assertEquals(new CancelResult(1L, false), first.get(3, TimeUnit.SECONDS));
      assertEquals(new CancelResult(2L, false), second.get(3, TimeUnit.SECONDS));
    } finally {
      gateway.close();
    }
  }

  @Test
  void boundsActiveRequestsCorrelatesOutOfOrderRepliesAndDrainsOnClose() throws Exception {
    CountDownLatch startPolling = new CountDownLatch(1);
    LinkedBlockingQueue<CommandRequest> offered = new LinkedBlockingQueue<>();
    ConcurrentLinkedQueue<CommandResponse> responses = new ConcurrentLinkedQueue<>();
    CompletableFuture<Integer> firstBatchSize = new CompletableFuture<>();
    AeronRequestClient client = new AeronRequestClient(null, null) {
      private final SbeRequestCodec codec = new SbeRequestCodec();
      private int offers;

      @Override
      public long trySend(DirectBuffer buffer, int offset, int length) {
        offered.add(codec.decode(buffer, offset, length));
        offers++;
        return 128;
      }

      @Override
      public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
        await(startPolling);
        if (offers >= 2) {
          firstBatchSize.complete(offers);
        }
        int work = 0;
        CommandResponse response;
        while (work < fragmentLimit && (response = responses.poll()) != null) {
          onResponse.accept(response);
          work++;
        }
        return work;
      }
    };
    EngineGateway gateway = new EngineGateway(client, 3, 2);
    gateway.start();
    try {
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      CompletableFuture<CommandResult> second = gateway.submit(request(2));
      CompletableFuture<CommandResult> third = gateway.submit(request(3));
      startPolling.countDown();
      assertEquals(2, firstBatchSize.get(3, TimeUnit.SECONDS));
      assertEquals(request(1), offered.poll(3, TimeUnit.SECONDS));
      assertEquals(request(2), offered.poll(3, TimeUnit.SECONDS));
      assertFalse(first.isDone());
      assertFalse(second.isDone());
      assertFalse(third.isDone());

      CompletableFuture<Void> closed = new CompletableFuture<>();
      Thread closer = closeOnThread(gateway, closed);
      awaitWaiting(closer);
      assertRejected(gateway.submit(request(4)));
      responses.add(new CommandResponse(request(2).requestId(), cancelResult(request(2))));
      assertEquals(cancelResult(request(2)), second.get(3, TimeUnit.SECONDS));
      assertEquals(request(3), offered.poll(3, TimeUnit.SECONDS));
      assertFalse(first.isDone(), "The second reply must not complete the first caller");
      assertFalse(closed.isDone(), "close must wait for the remaining active requests");

      responses.add(new CommandResponse(request(3).requestId(), cancelResult(request(3))));
      responses.add(new CommandResponse(request(1).requestId(), cancelResult(request(1))));
      closed.get(3, TimeUnit.SECONDS);
      assertEquals(cancelResult(request(1)), first.join());
      assertEquals(cancelResult(request(3)), third.join());
    } finally {
      startPolling.countDown();
      for (long id = 1; id <= 3; id++) {
        responses.add(new CommandResponse(request(id).requestId(), cancelResult(request(id))));
      }
      gateway.close();
    }
  }

  @Test
  void closeBeforeStartIsRepeatableAndPreventsStartup() {
    EngineGateway gateway = new EngineGateway(client(EngineGatewayTest::cancelResult), 1);
    gateway.close();
    gateway.close();
    assertThrows(IllegalStateException.class, gateway::start);
    assertRejected(gateway.submit(request(1)));
  }

  @Test
  void closeAfterStartIsRepeatableAndRejectsFurtherRequests() {
    EngineGateway gateway = new EngineGateway(client(EngineGatewayTest::cancelResult), 1);
    gateway.start();
    gateway.close();
    gateway.close();
    assertRejected(gateway.submit(request(1)));
    assertThrows(IllegalStateException.class, gateway::start);
  }

  @Test
  void rejectsOverflowWithoutDiscardingAcceptedRequests() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    EngineGateway gateway = blockedGateway(1, entered, release);
    gateway.start();
    try {
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      await(entered);
      CompletableFuture<CommandResult> second = gateway.submit(request(2));
      assertRejected(gateway.submit(request(3)));
      release.countDown();
      assertEquals(new CancelResult(1L, false), first.get(3, TimeUnit.SECONDS));
      assertEquals(new CancelResult(2L, false), second.get(3, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      gateway.close();
    }
  }

  @Test
  void diagnosticsDistinguishQueueOverflowFromLifecycleRejectionAndCountDrainedWork() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    GatewayDiagnostics diagnostics = new GatewayDiagnostics();
    EngineGateway gateway = blockedGateway(1, entered, release, diagnostics);
    assertRejected(gateway.submit(request(90)));
    gateway.start();
    try {
      var first = gateway.submit(request(1));
      await(entered);
      var second = gateway.submit(request(2));
      assertRejected(gateway.submit(request(3)));
      release.countDown();
      assertEquals(cancelResult(request(1)), first.get(3, TimeUnit.SECONDS));
      assertEquals(cancelResult(request(2)), second.get(3, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      gateway.close();
    }
    assertRejected(gateway.submit(request(91)));
    String report = diagnostics.summarize();
    for (String expected : List.of("Gateway accepted: 2", "Gateway activated: 2", "Gateway queue full: 1",
        "Gateway queue high-water (observed): 1", "Gateway active high-water: 1", "Gateway successful offers: 2",
        "Gateway retries: 0")) {
      assertTrue(report.contains(expected), report);
    }
    var wait = java.util.regex.Pattern.compile("Gateway queue wait mean: ([0-9.]+) us").matcher(report);
    assertTrue(wait.find(), report);
    assertTrue(Double.parseDouble(wait.group(1)) > 0, report);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void diagnosticsSeparateOfferTimeoutsFromReplyTimeoutsAndRetries(boolean offered) throws Exception {
    GatewayDiagnostics diagnostics = new GatewayDiagnostics();
    AeronRequestClient client = new AeronRequestClient(null, null, new ClientConfig(Duration.ofMillis(20), 2)) {
      @Override
      public long trySend(DirectBuffer buffer, int offset, int length) {
        return offered ? 128 : Publication.BACK_PRESSURED;
      }
      @Override
      public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
        return 0;
      }
    };
    try (EngineGateway gateway = new EngineGateway(client, 1, 1, diagnostics)) {
      gateway.start();
      var result = gateway.submit(request(1));
      assertThrows(ExecutionException.class, () -> result.get(3, TimeUnit.SECONDS));
    }
    String report = diagnostics.summarize();
    assertTrue(report.contains("Gateway successful offers: " + (offered ? 2 : 0)), report);
    assertTrue(report.contains("Gateway retries: " + (offered ? 1 : 0)), report);
    assertTrue(report.contains("Gateway reply timeouts: " + (offered ? 2 : 0)), report);
    assertTrue(report.contains("Gateway offer timeouts: " + (offered ? 0 : 1)), report);
  }

  @Test
  void concurrentClosesWaitForAcceptedWorkAndRejectNewSubmissions() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    EngineGateway gateway = blockedGateway(4, entered, release);
    gateway.start();
    try {
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      await(entered);
      CompletableFuture<CommandResult> second = gateway.submit(request(2));
      CompletableFuture<Void> firstClose = new CompletableFuture<>();
      Thread firstCloser = closeOnThread(gateway, firstClose);
      awaitWaiting(firstCloser);
      CompletableFuture<Void> secondClose = new CompletableFuture<>();
      Thread secondCloser = closeOnThread(gateway, secondClose);
      awaitWaiting(secondCloser);

      assertRejected(gateway.submit(request(3)));
      assertFalse(firstClose.isDone());
      assertFalse(secondClose.isDone());
      release.countDown();
      firstClose.get(3, TimeUnit.SECONDS);
      secondClose.get(3, TimeUnit.SECONDS);
      assertTrue(first.isDone(), "close must wait for the active request");
      assertTrue(second.isDone(), "close must wait for queued requests");
      assertEquals(new CancelResult(1L, false), first.join());
      assertEquals(new CancelResult(2L, false), second.join());
    } finally {
      release.countDown();
      gateway.close();
    }
  }

  @Test
  void reportsTerminalOfferFailureAndContinuesProcessing() throws Exception {
    AeronRequestClient client = new ReplyingAeronClient(EngineGatewayTest::cancelResult) {
      private final SbeRequestCodec codec = new SbeRequestCodec();

      @Override
      public long trySend(DirectBuffer buffer, int offset, int length) {
        if (codec.decode(buffer, offset, length).command().orderId() == 1L) {
          return Publication.MAX_POSITION_EXCEEDED;
        }
        return super.trySend(buffer, offset, length);
      }
    };
    EngineGateway gateway = new EngineGateway(client, 2);
    gateway.start();
    try {
      CompletableFuture<CommandResult> failed = gateway.submit(request(1));
      ExecutionException failure = assertThrows(ExecutionException.class, () -> failed.get(3, TimeUnit.SECONDS));
      assertInstanceOf(IllegalStateException.class, failure.getCause());
      assertTrue(failure.getCause().getMessage().contains(request(1).requestId().toString()));
      assertFalse(failure.getCause().getMessage().contains("outcome unknown"));
      assertEquals(new CancelResult(2L, false), gateway.submit(request(2)).get(3, TimeUnit.SECONDS));
    } finally {
      gateway.close();
    }
  }

  @Test
  void rejectsCloseFromWorkerCallbackWithoutStoppingTheGateway() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    EngineGateway gateway = blockedGateway(2, entered, release);
    gateway.start();
    try {
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      await(entered);
      CompletableFuture<Void> callback = first.thenRun(() -> assertThrows(IllegalStateException.class, gateway::close));
      release.countDown();
      callback.get(3, TimeUnit.SECONDS);
      assertEquals(new CancelResult(2L, false), gateway.submit(request(2)).get(3, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      gateway.close();
    }
  }

  @Test
  void interruptedCloserPreservesItsInterruptAndWorkerStillDrains() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    EngineGateway gateway = blockedGateway(2, entered, release);
    AtomicBoolean interrupted = new AtomicBoolean();
    CompletableFuture<Throwable> closeFailure = new CompletableFuture<>();
    gateway.start();
    try {
      CompletableFuture<CommandResult> first = gateway.submit(request(1));
      await(entered);
      CompletableFuture<CommandResult> second = gateway.submit(request(2));
      Thread closer = new Thread(() -> {
        try {
          gateway.close();
          closeFailure.complete(null);
        } catch (RuntimeException failure) {
          interrupted.set(Thread.currentThread().isInterrupted());
          closeFailure.complete(failure);
        }
      });
      closer.start();
      awaitWaiting(closer);
      closer.interrupt();
      Throwable failure = closeFailure.get(3, TimeUnit.SECONDS);
      assertInstanceOf(IllegalStateException.class, failure);
      assertInstanceOf(InterruptedException.class, failure.getCause());
      assertTrue(interrupted.get());
      assertRejected(gateway.submit(request(3)));
      release.countDown();
      gateway.close();
      assertEquals(new CancelResult(1L, false), first.join());
      assertEquals(new CancelResult(2L, false), second.join());
    } finally {
      release.countDown();
      gateway.close();
    }
  }

  @Test
  void racingSubmissionsAreEitherProcessedOrRejectedWithoutStrandedFutures() throws Exception {
    AtomicInteger processed = new AtomicInteger();
    EngineGateway gateway = new EngineGateway(client(request -> {
      processed.incrementAndGet();
      return cancelResult(request);
    }), 100);
    CountDownLatch race = new CountDownLatch(1);
    CompletableFuture<Void> closed = new CompletableFuture<>();
    Thread closer = new Thread(() -> {
      await(race);
      completeClose(gateway, closed);
    });
    gateway.start();
    try {
      List<CompletableFuture<CommandResult>> results = new ArrayList<>();
      results.add(gateway.submit(request(1)));
      closer.start();
      race.countDown();
      for (long id = 2; id <= 100; id++) {
        results.add(gateway.submit(request(id)));
      }
      closed.get(3, TimeUnit.SECONDS);
      int successes = 0;
      for (int index = 0; index < results.size(); index++) {
        CompletableFuture<CommandResult> result = results.get(index);
        assertTrue(result.isDone(), "close must not strand any submitted future");
        if (result.isCompletedExceptionally()) {
          assertRejected(result);
        } else {
          assertEquals(new CancelResult(index + 1L, false), result.join());
          successes++;
        }
      }
      assertEquals(successes, processed.get());
    } finally {
      race.countDown();
      gateway.close();
    }
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0L, id), new CancelOrder(id));
  }

  @Test
  void distinguishesQueueOverflowFromLifecycleRejection() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    EngineGateway gateway = blockedGateway(1, entered, release);
    gateway.start();
    try {
      gateway.submit(request(1));
      await(entered);
      gateway.submit(request(2));
      var failure = assertThrows(CompletionException.class, () -> gateway.submit(request(3)).join());
      assertInstanceOf(GatewayOverloadedException.class, failure.getCause());
    } finally {
      release.countDown();
      gateway.close();
    }
    var stopped = assertThrows(CompletionException.class, () -> gateway.submit(request(4)).join());
    assertFalse(stopped.getCause() instanceof GatewayOverloadedException);
  }

  private static CommandResult cancelResult(CommandRequest request) {
    return new CancelResult(request.command().orderId(), false);
  }

  // Keep real gateway threads/queues/futures; control only the external request/reply operation.
  private static AeronRequestClient client(Function<CommandRequest, CommandResult> send) {
    return new ReplyingAeronClient(send);
  }

  private static EngineGateway blockedGateway(int capacity, CountDownLatch entered, CountDownLatch release) {
    return blockedGateway(capacity, entered, release, null);
  }

  private static EngineGateway blockedGateway(int capacity, CountDownLatch entered, CountDownLatch release,
      GatewayDiagnostics diagnostics) {
    return new EngineGateway(client(request -> {
      if (request.command().orderId() == 1L) {
        entered.countDown();
        await(release);
      }
      return cancelResult(request);
    }), capacity, 1, diagnostics);
  }

  private static void assertRejected(CompletableFuture<CommandResult> result) {
    assertTrue(result.isCompletedExceptionally());
    CompletionException failure = assertThrows(CompletionException.class, result::join);
    assertInstanceOf(RejectedExecutionException.class, failure.getCause());
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(3, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for test coordination");
      }
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for test coordination", failure);
    }
  }

  private static Thread closeOnThread(EngineGateway gateway, CompletableFuture<Void> closed) {
    Thread closer = new Thread(() -> completeClose(gateway, closed));
    closer.start();
    return closer;
  }

  private static void completeClose(EngineGateway gateway, CompletableFuture<Void> closed) {
    try {
      gateway.close();
      closed.complete(null);
    } catch (RuntimeException failure) {
      closed.completeExceptionally(failure);
    }
  }

  private static void awaitWaiting(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (thread.isAlive() && thread.getState() != Thread.State.WAITING && System.nanoTime() - deadline < 0) {
      Thread.sleep(1);
    }
    assertEquals(Thread.State.WAITING, thread.getState(), "close should be waiting for the blocked worker");
  }
}
