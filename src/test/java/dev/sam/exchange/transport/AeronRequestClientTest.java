package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

class AeronRequestClientTest {
  @ParameterizedTest(name = "attempt limit: {0}")
  @ValueSource(ints = {1, 2, 4})
  @Timeout(10)
  void honorsConfiguredReplyTimeoutAndAttemptLimit(int attempts, @TempDir Path tempDir) throws Exception {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    // Close the Aeron resources before joining the worker if an assertion fails.
    try (ExecutorService executor = Executors.newSingleThreadExecutor();
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        Subscription commands = aeron.addSubscription("aeron:ipc", 1)) {
      awaitConnected(publication);
      Duration timeout = Duration.ofMillis(80);
      AeronRequestClient client = new AeronRequestClient(publication, replies, new ClientConfig(timeout, attempts));
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      List<CommandRequest> received = new ArrayList<>();
      SbeRequestCodec codec = new SbeRequestCodec();
      FragmentHandler handler = (buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length));
      long started = System.nanoTime();
      Future<CommandResult> result = executor.submit(() -> client.send(request));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();

      // Consume requests but deliberately never reply. The worker must use the short configured waits.
      while (!result.isDone()) {
        int fragments = commands.poll(handler, 10);
        assertTrue(System.nanoTime() - deadline < 0, "Configured reply timeout was ignored");
        idle.idle(fragments);
      }
      while (commands.poll(handler, 10) > 0) {
        // Collect any final request that became available as the worker completed.
      }
      long elapsed = System.nanoTime() - started;
      ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
      IllegalStateException cause = assertInstanceOf(IllegalStateException.class, failure.getCause());
      assertEquals(Collections.nCopies(attempts, request), received,
          "The configured number of attempts must preserve the original UUID and command");
      assertTrue(elapsed >= timeout.toNanos() * attempts, "Each attempt must receive its full configured reply wait");
      assertTrue(cause.getMessage().contains("No reply after " + attempts + " attempts"), cause.getMessage());
      assertTrue(cause.getMessage().contains(request.requestId().toString()), cause.getMessage());
      assertTrue(cause.getMessage().contains("outcome unknown"), cause.getMessage());
      assertFalse(publication.isClosed(), "The caller still owns the publication after a timeout");
      assertFalse(replies.isClosed(), "The caller still owns the subscription after a timeout");
      assertTrue(errors.isEmpty(), errors::toString);
    }
  }

  @Test
  @Timeout(10)
  void honorsConfiguredSendTimeoutWhenNoSubscriberConnects(@TempDir Path tempDir) {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {
      Duration timeout = Duration.ofMillis(80);
      AeronRequestClient client = new AeronRequestClient(publication, replies, new ClientConfig(timeout, 4));
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      long started = System.nanoTime();

      IllegalStateException failure = assertThrows(IllegalStateException.class, () -> client.send(request));

      long elapsed = System.nanoTime() - started;
      assertTrue(failure.getMessage().contains("Timed out sending order"), failure.getMessage());
      assertTrue(elapsed >= timeout.toNanos(), "The send must receive its full configured wait");
      assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2), "The send still used the old five-second deadline");
      assertTrue(errors.isEmpty(), errors::toString);
    }
  }

  @Test
  @Timeout(10)
  void acceptsReplyOnLastConfiguredAttemptAndCanSendAnotherRequest(@TempDir Path tempDir) throws Exception {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    try (ExecutorService executor = Executors.newSingleThreadExecutor();
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Publication responses = aeron.addPublication("aeron:ipc", 2)) {
      awaitConnected(publication);
      awaitConnected(responses);
      AeronRequestClient client = new AeronRequestClient(publication, replies,
          new ClientConfig(Duration.ofMillis(300), 2));
      CommandRequest first = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      List<CommandRequest> received = new ArrayList<>();
      SbeRequestCodec codec = new SbeRequestCodec();
      FragmentHandler handler = (buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length));
      Future<CommandResult> firstResult = executor.submit(() -> client.send(first));

      awaitRequests(commands, handler, received, 2, firstResult);
      assertEquals(List.of(first, first), received);
      sendResponse(responses, new CommandResponse(first.requestId(), new CancelResult(7L, false)));
      assertEquals(new CancelResult(7L, false), firstResult.get(2, TimeUnit.SECONDS));

      // A second call on the same client gets a fresh attempt budget and result list.
      CommandRequest next = new CommandRequest(new UUID(0L, 2L), new CancelOrder(8L));
      Future<CommandResult> nextResult = executor.submit(() -> client.send(next));
      awaitRequests(commands, handler, received, 4, nextResult);
      assertEquals(List.of(first, first, next, next), received);
      sendResponse(responses, new CommandResponse(next.requestId(), new CancelResult(8L, true)));
      assertEquals(new CancelResult(8L, true), nextResult.get(2, TimeUnit.SECONDS));
      assertFalse(publication.isClosed(), "Successful calls must leave the borrowed publication open");
      assertFalse(replies.isClosed(), "Successful calls must leave the borrowed subscription open");
      assertTrue(errors.isEmpty(), errors::toString);
    }
  }

  @ParameterizedTest(name = "retry publication closed: {0}")
  @ValueSource(booleans = {false, true})
  @Timeout(10)
  void sendFailureAfterAnAcceptedAttemptPreservesTheUnknownOutcome(boolean closePublication, @TempDir Path tempDir)
      throws Exception {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    try (ExecutorService executor = Executors.newSingleThreadExecutor();
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        Subscription commands = aeron.addSubscription("aeron:ipc", 1)) {
      awaitConnected(publication);
      AeronRequestClient client = new AeronRequestClient(publication, replies,
          new ClientConfig(Duration.ofMillis(500), 3));
      CommandRequest request = new CommandRequest(new UUID(0L, 1L), new CancelOrder(7L));
      List<CommandRequest> received = new ArrayList<>();
      SbeRequestCodec codec = new SbeRequestCodec();
      FragmentHandler handler = (buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length));
      Future<CommandResult> result = executor.submit(() -> client.send(request));
      awaitRequests(commands, handler, received, 1, result);
      assertEquals(List.of(request), received, "The initial send must reach the peer before it disappears");

      // The first command may have been processed. Withhold its reply, then make the resend fail.
      if (closePublication) {
        publication.close();
      } else {
        commands.close();
      }
      ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(3, TimeUnit.SECONDS));
      IllegalStateException cause = assertInstanceOf(IllegalStateException.class, failure.getCause());
      String expectedFailure = closePublication ? "Publication is closed" : "Timed out sending order";
      assertTrue(cause.getMessage().contains(expectedFailure), cause.getMessage());
      assertTrue(cause.getMessage().contains(request.requestId().toString()), cause.getMessage());
      assertTrue(cause.getMessage().contains("outcome unknown"), cause.getMessage());
      assertFalse(replies.isClosed(), "The client must leave the caller's subscription open after failure");
      assertTrue(errors.isEmpty(), errors::toString);
    }
  }

  private static void awaitConnected(Publication publication) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!publication.isConnected()) {
      assertTrue(System.nanoTime() - deadline < 0, "Test peer did not connect");
      idle.idle();
    }
  }

  private static void awaitRequests(Subscription commands, FragmentHandler handler, List<CommandRequest> received,
      int count, Future<CommandResult> result) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (received.size() < count) {
      int fragments = commands.poll(handler, 10);
      assertFalse(result.isDone(), "Client finished before the test peer could reply");
      assertTrue(System.nanoTime() - deadline < 0, "Client did not retry within its configured timeout");
      idle.idle(fragments);
    }
    assertEquals(count, received.size());
  }

  private static void sendResponse(Publication publication, CommandResponse response) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    int length = buffer.putStringAscii(0, new CommandResponseCodec().encode(response));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (publication.offer(buffer, 0, length) < 0) {
      assertTrue(System.nanoTime() - deadline < 0, "Test peer could not send its reply");
      idle.idle();
    }
  }
}
