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

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
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
  void keepsDriverAliveWhileClientDelaysReply(@TempDir Path tempDir) throws Exception {
    Path journalPath = tempDir.resolve("commands.journal");
    List<CommandResult> results = runServerSession(tempDir, journalPath,
        List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)), true);

    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        results);
    assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\n", Files.readString(journalPath));
  }

  @Test
  @Timeout(30)
  void recoversRemainingOrdersAfterServerRestart(@TempDir Path tempDir) throws Exception {
    Path journalPath = tempDir.resolve("journal/commands.journal");

    // The first server run leaves six lots resting on bid 1.
    List<CommandResult> firstResults = runServerSession(tempDir.resolve("first-run"), journalPath,
        List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)), false);
    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        firstResults);
    assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\n", Files.readString(journalPath));

    // A new server JVM must recover that bid from the same journal before processing ask 3.
    List<CommandResult> secondResults = runServerSession(tempDir.resolve("second-run"), journalPath,
        List.of(new PlaceOrder(3L, Side.ASK, 100L, 6L), new CancelOrder(1L)), false);
    assertEquals(List.of(new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 6L)), 0L), new CancelResult(1L, false)),
        secondResults);

    // Recovery must preserve the original commands and append only the two new ones.
    assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\nPLACE,3,ASK,100,6\nCANCEL,1\n", Files.readString(journalPath));
  }

  @Test
  @Timeout(30)
  void rejectsDuplicateOrderAndProcessesNextCommand(@TempDir Path tempDir) throws Exception {
    Path journalPath = tempDir.resolve("commands.journal");
    Files.writeString(journalPath, "PLACE,1,BID,100,10\n");

    // Recover bid 1, reject an attempt to change its price and size, then match a valid ask.
    List<CommandResult> results = runServerSession(tempDir, journalPath,
        List.of(new PlaceOrder(1L, Side.BID, 200L, 2L), new PlaceOrder(2L, Side.ASK, 99L, 4L)), false);
    assertEquals(List.of(new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)), results);

    // Only the accepted ask is appended; the rejected duplicate must never enter the journal.
    assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\n", Files.readString(journalPath));
  }

  @Test
  @Timeout(30)
  void servesAnotherClientAfterFirstClientDisconnects(@TempDir Path tempDir) throws Exception {
    Path journalPath = tempDir.resolve("commands.journal");

    // Each inner list uses a new client connection, but both talk to the same server JVM.
    List<List<CommandResult>> results = runServerWithClients(tempDir, journalPath,
        List.of(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)),
            List.of(new PlaceOrder(3L, Side.ASK, 100L, 6L), new CancelOrder(1L))),
        false);

    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        results.get(0));
    // The second client consumes the six lots left by the first; bid 1 is then gone.
    assertEquals(List.of(new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 6L)), 0L), new CancelResult(1L, false)),
        results.get(1));
    assertEquals("PLACE,1,BID,100,10\nPLACE,2,ASK,99,4\nPLACE,3,ASK,100,6\nCANCEL,1\n", Files.readString(journalPath));
  }

  @Test
  @Timeout(30)
  void shutsDownCleanlyWhileIdle(@TempDir Path tempDir) throws Exception {
    Path journalPath = tempDir.resolve("commands.journal");

    // No client ever connects: the server must still notice the shutdown request and close its driver.
    runServerWithClients(tempDir, journalPath, List.of(), false);

    assertEquals("", Files.readString(journalPath));
  }

  private static List<CommandResult> runServerSession(Path tempDir, Path journalPath, List<EngineCommand> orders,
      boolean delayFinalReply) throws Exception {
    return runServerWithClients(tempDir, journalPath, List.of(orders), delayFinalReply).getFirst();
  }

  private static List<List<CommandResult>> runServerWithClients(Path tempDir, Path journalPath,
      List<List<EngineCommand>> clientSessions, boolean delayFinalReply) throws Exception {
    Files.createDirectories(tempDir);
    Path serverLog = tempDir.resolve("server.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName(), journalPath.toString()).redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();

    try {
      awaitServerOutput(server, serverLog, "Server ready:");
      assertFalse(server.waitFor(250, TimeUnit.MILLISECONDS), "Server exited while waiting for a client");

      List<List<CommandResult>> results = new ArrayList<>();
      for (List<EngineCommand> orders : clientSessions) {
        results.add(exchangeCommands(server, serverLog, driverDirectory, orders, delayFinalReply));
        // The client's resources are closed, but the server must keep its book and driver alive.
        assertFalse(server.waitFor(250, TimeUnit.MILLISECONDS), "Server stopped after the client disconnected");
      }

      // On macOS/Linux, destroy sends SIGTERM, which runs the JVM shutdown hook.
      server.destroy();
      assertTrue(server.waitFor(12, TimeUnit.SECONDS), "Server did not stop after the shutdown request");
      String output = Files.readString(serverLog);
      // Signal termination can have a nonzero exit code; verify that application cleanup actually ran.
      assertFalse(output.contains("Server shutdown exceeded"), output);
      assertFalse(output.contains("Exception"), output);
      assertFalse(Files.exists(driverDirectory), "Shutdown did not remove the Aeron driver directory\n" + output);
      return List.copyOf(results);
    } finally {
      if (server.isAlive()) {
        // Emergency cleanup after a failed assertion; this never counts as a successful shutdown.
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  private static List<CommandResult> exchangeCommands(Process server, Path serverLog, Path driverDirectory,
      List<EngineCommand> orders, boolean delayFinalReply) throws Exception {
    List<CommandResult> received = new ArrayList<>();
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
      FragmentHandler handler = (replyBuffer, offset, length, header) -> received
          .add(resultCodec.decode(replyBuffer.getStringAscii(offset)));

      long connectionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!replies.isConnected()) {
        assertTrue(System.nanoTime() - connectionDeadline < 0, "Timed out connecting to the reply stream");
        idle.idle();
      }
      // Replies use one shared stream. A reconnect can expose an earlier client's last reply.
      // Discard only messages already present before this client sends its first command.
      while (replies.poll((replyBuffer, offset, length, header) -> {
      }, 10) > 0) {
      }

      for (int i = 0; i < orders.size(); i++) {
        int length = buffer.putStringAscii(0, commandCodec.encode(orders.get(i)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (commands.offer(buffer, 0, length) < 0) {
          assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test command");
          idle.idle();
        }

        if (delayFinalReply && i == orders.size() - 1) {
          // Wait until the final reply is queued, then deliberately delay consuming it.
          awaitServerOutput(server, serverLog, "Result: PlaceResult[orderId=2,");
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

      assertTrue(errors.isEmpty(), errors::toString);
    }
    return List.copyOf(received);
  }

  private static void awaitServerOutput(Process server, Path log, String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.readString(log).contains(expected)) {
      assertTrue(server.isAlive(), "Server exited before expected output\n" + Files.readString(log));
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for server output: " + expected);
      Thread.sleep(10);
    }
  }
}
