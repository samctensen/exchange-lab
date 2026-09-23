package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;

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
  void reportsSendFailureAndContinuesProcessing() throws Exception {
    IllegalStateException sendFailure = new IllegalStateException("Transport failed; outcome unknown");
    EngineGateway gateway = new EngineGateway(client(request -> {
      if (request.command().orderId() == 1L) {
        throw sendFailure;
      }
      return cancelResult(request);
    }), 2);
    gateway.start();
    try {
      CompletableFuture<CommandResult> failed = gateway.submit(request(1));
      ExecutionException failure = assertThrows(ExecutionException.class, () -> failed.get(3, TimeUnit.SECONDS));
      assertSame(sendFailure, failure.getCause());
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

  private static CommandResult cancelResult(CommandRequest request) {
    return new CancelResult(request.command().orderId(), false);
  }

  // Keep real gateway threads/queues/futures; control only the external request/reply operation.
  private static AeronRequestClient client(Function<CommandRequest, CommandResult> send) {
    return new AeronRequestClient(null, null) {
      @Override
      public CommandResult send(CommandRequest request) {
        return send.apply(request);
      }
    };
  }

  private static EngineGateway blockedGateway(int capacity, CountDownLatch entered, CountDownLatch release) {
    return new EngineGateway(client(request -> {
      if (request.command().orderId() == 1L) {
        entered.countDown();
        await(release);
      }
      return cancelResult(request);
    }), capacity);
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
