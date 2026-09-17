package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

class AeronEngineServerTest {
  @Test
  @Timeout(30)
  void keepsDriverAliveUntilClientReceivesFinalReply(@TempDir Path tempDir) throws Exception {
    Path serverLog = tempDir.resolve("server.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName()).redirectErrorStream(true).redirectOutput(serverLog.toFile()).start();

    try {
      awaitServerOutput(serverLog, "Server ready:");
      ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
      try (
          Aeron aeron = Aeron
              .connect(new Aeron.Context().aeronDirectoryName(driverDirectory.toString()).errorHandler(errors::add));
          Publication commands = aeron.addPublication("aeron:ipc", 1);
          Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {
        CommandCodec commandCodec = new CommandCodec();
        CommandResultCodec resultCodec = new CommandResultCodec();
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        List<CommandResult> received = new ArrayList<>();
        FragmentHandler handler = (replyBuffer, offset, length, header) -> received
            .add(resultCodec.decode(replyBuffer.getStringAscii(offset)));
        List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
            new PlaceOrder(2L, Side.ASK, 99L, 4L));

        for (int i = 0; i < orders.size(); i++) {
          int length = buffer.putStringAscii(0, commandCodec.encode(orders.get(i)));
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          while (commands.offer(buffer, 0, length) < 0) {
            assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test command");
            idle.idle();
          }

          if (i == orders.size() - 1) {
            // Wait until the final reply is queued, then deliberately delay consuming it.
            awaitServerOutput(serverLog, "Result: PlaceResult[orderId=2,");
            assertFalse(server.waitFor(750, TimeUnit.MILLISECONDS),
                "Server stopped its driver before the client consumed the final reply");
          }

          deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          while (received.size() <= i) {
            replies.poll(handler, 1);
            assertTrue(errors.isEmpty(), errors::toString);
            assertTrue(System.nanoTime() - deadline < 0, "Timed out receiving test reply");
            idle.idle();
          }
        }

        assertEquals(
            List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
            received);
        assertTrue(errors.isEmpty(), errors::toString);
      }

      // Closing the reply subscription lets the server shut down normally.
      assertTrue(server.waitFor(5, TimeUnit.SECONDS), "Server did not stop after the client disconnected");
      assertEquals(0, server.exitValue(), Files.readString(serverLog));
      assertFalse(Files.exists(driverDirectory));
      try (var files = Files.list(tempDir)) {
        Path journal = files.filter(path -> path.toString().endsWith(".journal")).findFirst().orElseThrow();
        assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\n", Files.readString(journal));
      }
    } finally {
      if (server.isAlive()) {
        server.destroy();
        if (!server.waitFor(3, TimeUnit.SECONDS)) {
          server.destroyForcibly();
          server.waitFor(3, TimeUnit.SECONDS);
        }
      }
    }
  }

  private static void awaitServerOutput(Path log, String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.readString(log).contains(expected)) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for server output: " + expected);
      Thread.sleep(10);
    }
  }
}
