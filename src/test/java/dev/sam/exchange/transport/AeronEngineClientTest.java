package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

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
  @Test
  @Timeout(30)
  void waitsForMatchingRequestIdAndIgnoresUnrelatedReplies(@TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    Path clientLog = tempDir.resolve("client.log");
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    // A real Aeron peer controls reply order while the actual demo client runs in another JVM.
    try (
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {
      String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
      Process client = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
          AeronEngineClient.class.getName()).redirectErrorStream(true).redirectOutput(clientLog.toFile()).start();

      try {
        List<String> messages = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> messages.add(buffer.getStringAscii(offset));
        CommandRequestCodec requestCodec = new CommandRequestCodec();

        awaitCommandCount(commands, handler, messages, 1, client, clientLog);
        assertTrue(messages.getFirst().startsWith("REQUEST,"), "Client must send a request wrapper");
        CommandRequest first = requestCodec.decode(messages.getFirst());
        assertEquals(new PlaceOrder(1L, Side.BID, 100L, 10L), first.command());

        UUID unrelatedId = new UUID(first.requestId().getMostSignificantBits() ^ 1L,
            first.requestId().getLeastSignificantBits());
        sendResponse(replies, new CommandResponse(unrelatedId, new CancelResult(999L, false)));
        assertStillWaiting(commands, handler, messages, 1, client, clientLog);

        sendResponse(replies, new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L)));
        awaitCommandCount(commands, handler, messages, 2, client, clientLog);
        CommandRequest second = requestCodec.decode(messages.get(1));
        assertEquals(new PlaceOrder(2L, Side.ASK, 99L, 4L), second.command());
        assertNotEquals(first.requestId(), second.requestId(), "Each command needs its own request ID");

        // Even a previously valid reply must not complete the next request.
        sendResponse(replies, new CommandResponse(first.requestId(), new PlaceResult(1L, List.of(), 10L)));
        assertStillWaiting(commands, handler, messages, 2, client, clientLog);
        sendResponse(replies,
            new CommandResponse(second.requestId(), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)));

        assertTrue(client.waitFor(5, TimeUnit.SECONDS), "Client did not finish after its matching replies");
        String output = Files.readString(clientLog);
        assertEquals(0, client.exitValue(), output);
        assertEquals(List.of("PlaceResult[orderId=1, trades=[], remainingLots=10]",
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

  private static void awaitCommandCount(Subscription commands, FragmentHandler handler, List<String> messages,
      int expected, Process client, Path clientLog) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
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
      assertEquals(expected, messages.size(), "Client advanced after an unrelated reply");
      assertTrue(client.isAlive(), "Client finished after an unrelated reply\n" + Files.readString(clientLog));
      idle.idle();
    }
  }

  private static void sendResponse(Publication replies, CommandResponse response) {
    UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
    int length = buffer.putStringAscii(0, new CommandResponseCodec().encode(response));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (replies.offer(buffer, 0, length) < 0) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test response");
      idle.idle();
    }
  }
}
