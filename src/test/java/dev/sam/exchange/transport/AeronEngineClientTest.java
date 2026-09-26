package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

class AeronEngineClientTest {
  @Test
  @Timeout(30)
  void gatewayBatchMatchesAndIsRecordedByTheRealServer(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    Path serverLog = tempDir.resolve("server.log");
    Path clientLog = tempDir.resolve("client.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName(), archiveDirectory.toString()).redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();
    Process client = null;
    try {
      awaitServerReady(server, serverLog);
      client = startClient(tempDir, clientLog);
      assertTrue(client.waitFor(10, TimeUnit.SECONDS), "Gateway client did not finish\n" + Files.readString(clientLog));
      String output = Files.readString(clientLog);
      assertEquals(0, client.exitValue(), output);
      assertEquals(List.of("PlaceResult[orderId=1, trades=[], remainingLots=10]",
          "PlaceResult[orderId=2, trades=[Trade[incomingOrderId=2, restingOrderId=1, priceTicks=100, quantityLots=4]], remainingLots=0]"),
          output.lines().filter(line -> line.startsWith("PlaceResult[")).toList());
      assertFalse(output.contains("Exception"), output);
      assertTrue(server.isAlive(), "Client shutdown must leave the exchange running");

      // Stop the real server cleanly before reopening its Archive to verify the recorded batch.
      server.destroy();
      assertTrue(server.waitFor(12, TimeUnit.SECONDS), "Server did not shut down cleanly");
      String serverOutput = Files.readString(serverLog);
      assertFalse(serverOutput.contains("Exception"), serverOutput);
      assertFalse(serverOutput.contains("Server shutdown exceeded"), serverOutput);
      assertFalse(Files.exists(tempDir.resolve("exchange-lab-aeron")), serverOutput);
      List<CommandRequest> recorded = ArchiveTestSupport.readAll(archiveDirectory);
      assertEquals(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)),
          recorded.stream().map(CommandRequest::command).toList());
      assertNotEquals(recorded.get(0).requestId(), recorded.get(1).requestId());
    } finally {
      if (client != null && client.isAlive()) {
        client.destroyForcibly();
        client.waitFor(3, TimeUnit.SECONDS);
      }
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  @ParameterizedTest(name = "fragmented replies: {0}")
  @ValueSource(booleans = {false, true})
  @Timeout(30)
  void waitsForMatchingRequestIdAndIgnoresUnrelatedReplies(boolean fragmentedReplies, @TempDir Path tempDir)
      throws Exception {
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Path clientLog = tempDir.resolve("client.log");
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    // A real Aeron peer controls reply order while the actual demo client runs in another JVM.
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(driverDirectory.toString()).ipcMtuLength(256)
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {
      Process client = startClient(tempDir, clientLog);

      try {
        List<byte[]> messages = new ArrayList<>();
        FragmentHandler handler = captureRequests(messages);

        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        CommandRequest first = decodeRequest(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Ten 32-byte trade entries plus the 44-byte prefix exceed this publication's single-fragment payload.
        List<Trade> firstTrades = new ArrayList<>();
        if (fragmentedReplies) {
          for (int i = 0; i < 10; i++) {
            firstTrades.add(new Trade(1L, Long.MAX_VALUE - i, 100L, 1L));
          }
        }
        PlaceResult firstResult = new PlaceResult(1L, firstTrades, fragmentedReplies ? 0L : 10L);

        UUID unrelatedId = new UUID(first.requestId().getMostSignificantBits() ^ 1L,
            first.requestId().getLeastSignificantBits());
        sendResponse(replies, new CommandResponse(unrelatedId, new CancelResult(999L, false)));
        if (fragmentedReplies) {
          int length = sendResponse(replies, new CommandResponse(unrelatedId, firstResult));
          assertTrue(length > replies.maxPayloadLength(), "The test must send a fragmented reply");
          assertTrue(length <= replies.maxMessageLength(), "The fixture must fit within Aeron's message limit");
        }
        assertStillWaiting(commands, handler, messages, 1, client, clientLog);

        sendResponse(replies, new CommandResponse(first.requestId(), firstResult));
        awaitCommandCount(commands, handler, messages, 2, client, clientLog);
        CommandRequest second = decodeRequest(messages.get(1));
        assertEquals(new PlaceOrder(2L, Side.ASK, 99L, 4L), second.command());
        assertNotEquals(first.requestId(), second.requestId(), "Each command needs its own request ID");

        // Even a previously valid reply must not complete the next request.
        sendResponse(replies, new CommandResponse(first.requestId(), firstResult));
        assertStillWaiting(commands, handler, messages, 2, client, clientLog);
        sendResponse(replies,
            new CommandResponse(second.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)));

        assertTrue(client.waitFor(5, TimeUnit.SECONDS), "Client did not finish after its matching replies");
        String output = Files.readString(clientLog);
        assertEquals(0, client.exitValue(), output);
        assertEquals(List.of(firstResult.toString(),
            "PlaceResult[orderId=2, trades=[Trade[incomingOrderId=2, restingOrderId=1, priceTicks=100, quantityLots=4]], remainingLots=0]"),
            output.lines().filter(line -> line.startsWith("PlaceResult[")).toList());
        assertFalse(output.contains("CancelResult[orderId=999"), "Client printed an unrelated reply\n" + output);
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        if (client.isAlive()) {
          client.destroyForcibly();
          client.waitFor(3, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Test
  @Timeout(30)
  void retriesTheSameRequestAndAcceptsADelayedReplyOnce(@TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Path clientLog = tempDir.resolve("client.log");
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    try (
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {
      Process client = startClient(tempDir, clientLog);
      try {
        List<byte[]> messages = new ArrayList<>();
        FragmentHandler handler = captureRequests(messages);
        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        CommandRequest first = decodeRequest(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Wait briefly to catch immediate resends, then withhold the reply until the timeout.
        assertStillWaiting(commands, handler, messages, 1, client, clientLog);
        awaitCommandCount(commands, handler, messages, 2, client, clientLog, 8);
        assertArrayEquals(messages.getFirst(), messages.get(1), "A retry must preserve the UUID and command bytes");
        CommandResponse delayed = new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L));
        sendResponse(replies, delayed);
        awaitCommandCount(commands, handler, messages, 3, client, clientLog);
        CommandRequest next = decodeRequest(messages.get(2));
        assertEquals(new PlaceOrder(2L, Side.ASK, 99L, 4L), next.command());
        assertNotEquals(first.requestId(), next.requestId(), "The next logical order needs a fresh UUID");

        // The reply generated by the other attempt may arrive while the next order is pending.
        sendResponse(replies, delayed);
        assertStillWaiting(commands, handler, messages, 3, client, clientLog);
        sendResponse(replies,
            new CommandResponse(next.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)));

        assertTrue(client.waitFor(5, TimeUnit.SECONDS), "Client did not finish after the matching reply");
        String output = Files.readString(clientLog);
        assertEquals(0, client.exitValue(), output);
        assertEquals(List.of("PlaceResult[orderId=1, trades=[], remainingLots=10]",
            "PlaceResult[orderId=2, trades=[Trade[incomingOrderId=2, restingOrderId=1, priceTicks=100, quantityLots=4]], remainingLots=0]"),
            output.lines().filter(line -> line.startsWith("PlaceResult[")).toList());
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        if (client.isAlive()) {
          client.destroyForcibly();
          client.waitFor(3, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Test
  @Timeout(30)
  void drainsAcceptedBatchAfterRequestTimeoutAndReportsUnknownOutcome(@TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Path clientLog = tempDir.resolve("client.log");
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    try (
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {
      Process client = startClient(tempDir, clientLog);
      try {
        List<byte[]> messages = new ArrayList<>();
        FragmentHandler handler = captureRequests(messages);
        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        CommandRequest first = decodeRequest(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Withhold all replies to the first request. After its three attempts, graceful close must
        // drain the second request, which the demo already accepted into the gateway's queue.
        awaitCommandCount(commands, handler, messages, 4, client, clientLog, 18);
        for (int attempt = 1; attempt < 3; attempt++) {
          assertArrayEquals(messages.getFirst(), messages.get(attempt), "Retries must preserve the original bytes");
        }
        CommandRequest second = decodeRequest(messages.get(3));
        assertEquals(new PlaceOrder(2L, Side.ASK, 99L, 4L), second.command());
        assertNotEquals(first.requestId(), second.requestId());

        // Main has observed the first failure, but must keep Aeron open until this accepted work finishes.
        assertStillWaiting(commands, handler, messages, 4, client, clientLog);
        sendResponse(replies,
            new CommandResponse(second.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)));
        assertTrue(client.waitFor(5, TimeUnit.SECONDS), "Client did not finish draining the accepted batch");
        String output = Files.readString(clientLog);
        assertNotEquals(0, client.exitValue(), "An unanswered request must not look successful");
        assertTrue(output.contains("No reply after 3 attempts"), output);
        assertTrue(output.contains(first.requestId().toString()), output);
        assertTrue(output.contains("outcome unknown"), output);
        assertFalse(output.contains("PlaceResult["),
            "The first failed join should abort the result-printing loop\n" + output);
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        if (client.isAlive()) {
          client.destroyForcibly();
          client.waitFor(3, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static void awaitServerReady(Process server, Path log) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.readString(log).contains("Server ready:")) {
      assertTrue(server.isAlive(), "Server exited before becoming ready\n" + Files.readString(log));
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for the server\n" + Files.readString(log));
      Thread.sleep(10);
    }
  }

  private static Process startClient(Path tempDir, Path clientLog) throws IOException {
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineClient.class.getName()).redirectErrorStream(true).redirectOutput(clientLog.toFile()).start();
  }

  private static FragmentHandler captureRequests(List<byte[]> messages) {
    return (buffer, offset, length, header) -> {
      byte[] message = new byte[length];
      buffer.getBytes(offset, message);
      messages.add(message);
    };
  }

  private static CommandRequest decodeRequest(byte[] message) {
    return new SbeRequestCodec().decode(new UnsafeBuffer(message), 0, message.length);
  }

  private static void awaitCommandCount(Subscription commands, FragmentHandler handler, List<byte[]> messages,
      int expected, Process client, Path clientLog) throws Exception {
    awaitCommandCount(commands, handler, messages, expected, client, clientLog, 5);
  }

  private static void awaitCommandCount(Subscription commands, FragmentHandler handler, List<byte[]> messages,
      int expected, Process client, Path clientLog, long timeoutSeconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (messages.size() < expected) {
      commands.poll(handler, 1);
      assertTrue(client.isAlive(), "Client exited before sending its command\n" + Files.readString(clientLog));
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for client command " + expected);
      idle.idle();
    }
    assertEquals(expected, messages.size());
  }

  private static void assertStillWaiting(Subscription commands, FragmentHandler handler, List<byte[]> messages,
      int expected, Process client, Path clientLog) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (System.nanoTime() - deadline < 0) {
      commands.poll(handler, 1);
      assertEquals(expected, messages.size(), "Client sent another command while still waiting for a matching reply");
      assertTrue(client.isAlive(),
          "Client finished while still waiting for a matching reply\n" + Files.readString(clientLog));
      idle.idle();
    }
  }

  private static int sendResponse(Publication replies, CommandResponse response) {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    int length = new SbeResponseCodec().encode(response, buffer, 0);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (replies.offer(buffer, 0, length) < 0) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test response");
      idle.idle();
    }
    return length;
  }
}
