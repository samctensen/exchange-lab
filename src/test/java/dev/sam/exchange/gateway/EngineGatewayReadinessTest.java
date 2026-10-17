package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

@Timeout(15)
class EngineGatewayReadinessTest {
  @Test
  void requiresStartedWorkerAndConnectedTransport(@TempDir Path directory) {
    try (Transport transport = new Transport(directory);
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(transport.requests, transport.replies), 1)) {
      assertFalse(gateway.isReady());
      gateway.start();
      assertFalse(gateway.isReady());
      try (Subscription commands = transport.aeron.addSubscription("aeron:ipc", 1)) {
        await(gateway::isReady);
        commands.close();
        await(() -> !gateway.isReady());
      }
      try (Subscription commands = transport.aeron.addSubscription("aeron:ipc", 1)) {
        await(gateway::isReady);
        gateway.close();
        assertFalse(gateway.isReady());
        assertFalse(transport.requests.isClosed());
        assertFalse(transport.replies.isClosed());
      }
    }
  }

  @Test
  void queueSaturationStaysReadyButDrainingDoesNot(@TempDir Path directory) throws Exception {
    CountDownLatch polling = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (Transport transport = new Transport(directory);
        Subscription commands = transport.aeron.addSubscription("aeron:ipc", 1)) {
      var client = new AeronRequestClient(transport.requests, transport.replies,
          new ClientConfig(Duration.ofMillis(50), 1)) {
        @Override
        public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
          polling.countDown();
          awaitLatch(release);
          return super.pollResponses(onResponse, fragmentLimit);
        }
      };
      try (EngineGateway gateway = new EngineGateway(client, 1)) {
        gateway.start();
        awaitLatch(polling);
        await(gateway::isReady);
        var accepted = gateway.submit(request(1));
        var rejected = gateway.submit(request(2));
        assertTrue(rejected.isCompletedExceptionally(), "The single queue slot is occupied");
        assertTrue(gateway.isReady(), "Queue capacity is admission control, not transport readiness");
        Thread closing = Thread.startVirtualThread(gateway::close);
        try {
          await(() -> !gateway.isReady());
          assertTrue(closing.isAlive(), "The worker is still draining the accepted request");
          assertFalse(accepted.isDone());
        } finally {
          release.countDown();
          closing.join(3000);
        }
        assertFalse(closing.isAlive());
        assertTrue(accepted.isCompletedExceptionally());
        assertFalse(gateway.isReady());
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void workerFailureRemovesReadinessEvenWithConnectedTransport(@TempDir Path directory) throws Exception {
    CountDownLatch polling = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    var failure = new CompletableFuture<Throwable>();
    try (Transport transport = new Transport(directory);
        Subscription commands = transport.aeron.addSubscription("aeron:ipc", 1)) {
      var client = new AeronRequestClient(transport.requests, transport.replies) {
        @Override
        public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
          Thread.currentThread().setUncaughtExceptionHandler((thread, error) -> failure.complete(error));
          polling.countDown();
          awaitLatch(release);
          throw new IllegalStateException("Test transport failure");
        }
      };
      try (EngineGateway gateway = new EngineGateway(client, 1)) {
        gateway.start();
        awaitLatch(polling);
        await(gateway::isReady);
        release.countDown();
        assertInstanceOf(IllegalStateException.class, failure.get(3, TimeUnit.SECONDS));
        assertTrue(client.isTransportAvailable());
        assertFalse(gateway.isReady());
      } finally {
        release.countDown();
      }
    }
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0, id), new CancelOrder(id));
  }

  private static void await(BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    var idle = new SleepingIdleStrategy();
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() - deadline < 0, "Readiness did not converge");
      idle.idle();
    }
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertTrue(latch.await(3, TimeUnit.SECONDS), "Worker coordination timed out");
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new AssertionError(error);
    }
  }

  private static final class Transport implements AutoCloseable {
    final MediaDriver driver;
    final Aeron aeron;
    final Publication requests;
    final Subscription replies;

    Transport(Path directory) {
      driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
          .aeronDirectoryName(directory.resolve("driver").toString()).dirDeleteOnShutdown(true));
      aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
      requests = aeron.addPublication("aeron:ipc", 1);
      replies = aeron.addSubscription("aeron:ipc", 2);
    }

    @Override
    public void close() {
      aeron.close();
      driver.close();
    }
  }
}
