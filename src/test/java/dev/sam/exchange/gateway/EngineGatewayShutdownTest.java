package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import io.aeron.Publication;

@Timeout(10)
class EngineGatewayShutdownTest {
  @ParameterizedTest(name = "worker throws instead of interrupting: {0}")
  @ValueSource(booleans = {false, true})
  void workerExitFailsSentUnsentAndQueuedRequests(boolean throwFailure) throws Exception {
    CountDownLatch startPolling = new CountDownLatch(1);
    CountDownLatch secondOffer = new CountDownLatch(1);
    CountDownLatch stopWorker = new CountDownLatch(1);
    CompletableFuture<Throwable> workerFailure = new CompletableFuture<>();
    AeronRequestClient client = new AeronRequestClient(null, null) {
      private int offers;

      @Override
      public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
        await(startPolling);
        return 0;
      }

      @Override
      public long trySend(DirectBuffer buffer, int offset, int length) {
        if (++offers == 1) {
          return 128;
        }
        secondOffer.countDown();
        await(stopWorker);
        if (throwFailure) {
          Thread.currentThread().setUncaughtExceptionHandler((thread, failure) -> workerFailure.complete(failure));
          throw new IllegalStateException("Simulated transport failure");
        }
        Thread.currentThread().interrupt();
        return Publication.BACK_PRESSURED;
      }
    };
    EngineGateway gateway = new EngineGateway(client, 3, 2);
    gateway.start();
    try {
      CompletableFuture<CommandResult> sent = gateway.submit(request(1));
      CompletableFuture<CommandResult> unsent = gateway.submit(request(2));
      CompletableFuture<CommandResult> queued = gateway.submit(request(3));
      CompletableFuture<Void> callback = sent.handle((result, failure) -> {
        assertInstanceOf(RejectedExecutionException.class, failureOf(gateway.submit(request(4))));
        return null;
      });
      startPolling.countDown();
      await(secondOffer);
      stopWorker.countDown();
      gateway.close();

      Throwable sentFailure = failureOf(sent);
      assertInstanceOf(IllegalStateException.class, sentFailure);
      assertTrue(sentFailure.getMessage().contains(request(1).requestId().toString()));
      assertTrue(sentFailure.getMessage().contains("outcome unknown"));
      Throwable unsentFailure = failureOf(unsent);
      assertInstanceOf(IllegalStateException.class, unsentFailure);
      assertTrue(unsentFailure.getMessage().contains(request(2).requestId().toString()));
      assertFalse(unsentFailure.getMessage().contains("outcome unknown"));
      Throwable queuedFailure = failureOf(queued);
      assertInstanceOf(IllegalStateException.class, queuedFailure);
      assertFalse(queuedFailure.getMessage().contains("outcome unknown"));
      callback.get(3, TimeUnit.SECONDS);
      if (throwFailure) {
        assertEquals("Simulated transport failure", workerFailure.get(3, TimeUnit.SECONDS).getMessage());
      }
    } finally {
      startPolling.countDown();
      stopWorker.countDown();
      gateway.close();
    }
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0L, id), new CancelOrder(id));
  }

  private static Throwable failureOf(CompletableFuture<CommandResult> result) {
    assertTrue(result.isCompletedExceptionally(), "Worker exit must not leave callers waiting");
    return assertThrows(CompletionException.class, result::join).getCause();
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
}
