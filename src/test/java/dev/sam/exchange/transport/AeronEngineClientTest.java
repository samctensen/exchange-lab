package dev.sam.exchange.transport;

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
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

class AeronEngineClientTest {
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
        List<String> messages = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> messages.add(buffer.getStringAscii(offset));
        CommandRequestCodec requestCodec = new CommandRequestCodec();

        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        assertTrue(messages.getFirst().startsWith("REQUEST,"), "Client must send a request wrapper");
        CommandRequest first = requestCodec.decode(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Ten one-lot fills with long resting IDs exceed this publication's single-fragment payload.
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
        CommandRequest second = requestCodec.decode(messages.get(1));
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
        List<String> messages = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> messages.add(buffer.getStringAscii(offset));
        CommandRequestCodec codec = new CommandRequestCodec();
        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        CommandRequest first = codec.decode(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Wait briefly to catch immediate resends, then withhold the reply until the timeout.
        assertStillWaiting(commands, handler, messages, 1, client, clientLog);
        awaitCommandCount(commands, handler, messages, 2, client, clientLog, 8);
        assertEquals(messages.getFirst(), messages.get(1), "A retry must preserve the UUID and command bytes");
        CommandResponse delayed = new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L));
        sendResponse(replies, delayed);
        awaitCommandCount(commands, handler, messages, 3, client, clientLog);
        CommandRequest next = codec.decode(messages.get(2));
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
  void stopsAfterThreeUnansweredAttemptsAndReportsUnknownOutcome(@TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Path clientLog = tempDir.resolve("client.log");
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    try (
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1)) {
      Process client = startClient(tempDir, clientLog);
      try {
        List<String> messages = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> messages.add(buffer.getStringAscii(offset));
        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        CommandRequest first = new CommandRequestCodec().decode(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        // Receive every attempt, but never publish a reply. Keep polling until the client exits.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(22);
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        while (client.isAlive()) {
          int fragments = commands.poll(handler, 10);
          assertTrue(messages.size() <= 3, "Client exceeded its three-attempt limit: " + messages);
          assertTrue(System.nanoTime() - deadline < 0, "Client did not stop after exhausting its attempts");
          idle.idle(fragments);
        }
        String output = Files.readString(clientLog);
        assertEquals(List.of(messages.getFirst(), messages.getFirst(), messages.getFirst()), messages,
            "Send exactly three copies of the first request, then stop before submitting the next order");
        assertNotEquals(0, client.exitValue(), "An unanswered request must not look successful");
        assertTrue(output.contains("No reply after 3 attempts"), output);
        assertTrue(output.contains(first.requestId().toString()), output);
        assertTrue(output.contains("outcome unknown"), output);
        assertFalse(output.contains("PlaceResult["), "Client printed a result it never received\n" + output);
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        if (client.isAlive()) {
          client.destroyForcibly();
          client.waitFor(3, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static Process startClient(Path tempDir, Path clientLog) throws IOException {
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineClient.class.getName()).redirectErrorStream(true).redirectOutput(clientLog.toFile()).start();
  }

  private static void awaitCommandCount(Subscription commands, FragmentHandler handler, List<String> messages,
      int expected, Process client, Path clientLog) throws Exception {
    awaitCommandCount(commands, handler, messages, expected, client, clientLog, 5);
  }

  private static void awaitCommandCount(Subscription commands, FragmentHandler handler, List<String> messages,
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

  private static void assertStillWaiting(Subscription commands, FragmentHandler handler, List<String> messages,
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
    int length = buffer.putStringAscii(0, new CommandResponseCodec().encode(response));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (replies.offer(buffer, 0, length) < 0) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test response");
      idle.idle();
    }
    return length;
  }
}
