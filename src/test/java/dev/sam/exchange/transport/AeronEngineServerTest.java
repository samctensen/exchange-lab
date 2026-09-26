package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.archive.client.AeronArchive;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;

class AeronEngineServerTest {
  private static final int IPC_MTU_LENGTH = 1408;

  @Test
  @Timeout(30)
  void rejectsRequestIdConflictAndKeepsServingRequests(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    UUID originalId = new UUID(0L, 1L);
    CommandRequest original = new CommandRequest(originalId, new PlaceOrder(1L, Side.ASK, 100L, 5L));
    CommandRequest conflict = new CommandRequest(originalId, new PlaceOrder(99L, Side.BID, 100L, 2L));
    CommandRequest next = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(2L, Side.BID, 100L, 2L));
    CommandRequest finalFill = new CommandRequest(new UUID(0L, 3L), new PlaceOrder(3L, Side.BID, 100L, 3L));

    // The conflicting bid must neither trade nor replace the original ask's cached response.
    List<List<CommandResult>> results = runServerWithRequests(tempDir, archiveDirectory,
        List.of(List.of(original, conflict, original, next, finalFill)), false, false);

    assertEquals(List.of(new PlaceResult(1L, List.of(), 5L), new RejectResult(99L, RejectReason.REQUEST_ID_CONFLICT),
        new PlaceResult(1L, List.of(), 5L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L),
        new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 3L)), 0L)), results.getFirst());
    assertEquals(List.of(original, next, finalFill), ArchiveTestSupport.readAll(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void retryAfterClientReconnectDoesNotMatchTheFilledOrderAgain(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    CommandRequest ask = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.ASK, 100L, 5L));
    CommandRequest bid = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(2L, Side.BID, 100L, 2L));
    CommandRequest finalFill = new CommandRequest(new UUID(0L, 3L), new PlaceOrder(3L, Side.BID, 100L, 3L));

    // A new client retries the already-filled bid using the same UUID, then consumes the remaining three lots.
    List<List<CommandResult>> results = runServerWithRequests(tempDir, archiveDirectory,
        List.of(List.of(ask, bid), List.of(bid, finalFill)), false, false);

    PlaceResult originalFill = new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 2L)), 0L);
    assertEquals(List.of(new PlaceResult(1L, List.of(), 5L), originalFill), results.get(0));
    assertEquals(List.of(originalFill, new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 3L)), 0L)), results.get(1));
    assertEquals(List.of(ask, bid, finalFill), ArchiveTestSupport.readAll(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void returnsEveryTradeWhenReplyExceedsTheOriginalBufferAndFragmentSize(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    List<EngineCommand> initialOrders = new ArrayList<>();
    List<CommandRequest> initialRequests = new ArrayList<>();
    List<Trade> expectedTrades = new ArrayList<>();
    for (long orderId = 1; orderId <= 100; orderId++) {
      PlaceOrder ask = new PlaceOrder(orderId, Side.ASK, 100L, 1L);
      initialOrders.add(ask);
      initialRequests.add(new CommandRequest(new UUID(0L, orderId), ask));
      expectedTrades.add(new Trade(1000L, orderId, 100L, 1L));
    }

    ArchiveTestSupport.record(archiveDirectory, initialRequests);
    PlaceResult expected = new PlaceResult(1000L, expectedTrades, 0L);
    String encoded = new CommandResponseCodec().encode(new CommandResponse(UUID.randomUUID(), expected));
    assertTrue(encoded.length() > 256, "The reply must exceed the old server buffer");
    assertTrue(encoded.length() > IPC_MTU_LENGTH, "The reply must require multiple fragments");

    // A single bid sweeps all 100 resting asks; the UUID-filtering test peer reassembles its reply.
    List<CommandResult> results = runServerSession(tempDir, archiveDirectory,
        List.of(new PlaceOrder(1000L, Side.BID, 100L, 100L), new CancelOrder(100L)), false);

    assertEquals(List.of(expected, new CancelResult(100L, false)), results);
    List<EngineCommand> expectedCommands = new ArrayList<>(initialOrders);
    expectedCommands.add(new PlaceOrder(1000L, Side.BID, 100L, 100L));
    expectedCommands.add(new CancelOrder(100L));
    assertEquals(expectedCommands, recordedCommands(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void waitsForCompleteFragmentedRequestBeforeProcessingIt(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    List<List<CommandResult>> results = runServerWithClients(tempDir, archiveDirectory,
        List.of(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L))), false, true);

    assertEquals(List.of(List.of(new PlaceResult(1L, List.of(), 10L))), results);
    assertEquals(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L)), recordedCommands(archiveDirectory),
        "Process and log the request exactly once");
  }

  @Test
  @Timeout(30)
  void keepsDriverAliveWhileClientDelaysReply(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    List<CommandResult> results = runServerSession(tempDir, archiveDirectory,
        List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)), true);

    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        results);
    assertEquals(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)),
        recordedCommands(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void recoversRemainingOrdersAndCachedRepliesAfterServerRestart(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("nested/archive");
    CommandRequest bid = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
    CommandRequest ask = new CommandRequest(new UUID(0L, 2L), new PlaceOrder(2L, Side.ASK, 99L, 4L));
    CommandRequest finalFill = new CommandRequest(new UUID(0L, 3L), new PlaceOrder(3L, Side.ASK, 100L, 6L));
    CommandRequest cancel = new CommandRequest(new UUID(0L, 4L), new CancelOrder(1L));
    PlaceResult originalFill = new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L);

    // The first server run leaves six lots resting on bid 1.
    List<List<CommandResult>> firstResults = runServerWithRequests(tempDir.resolve("first-run"), archiveDirectory,
        List.of(List.of(bid, ask)), false, false);
    assertEquals(List.of(List.of(new PlaceResult(1L, List.of(), 10L), originalFill)), firstResults);
    assertEquals(List.of(bid, ask), ArchiveTestSupport.readAll(archiveDirectory));

    // A new JVM must remember the filled ask's reply before accepting a retry of that same UUID.
    List<List<CommandResult>> secondResults = runServerWithRequests(tempDir.resolve("second-run"), archiveDirectory,
        List.of(List.of(ask, finalFill, cancel)), false, false);
    assertEquals(List.of(List.of(originalFill, new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 6L)), 0L),
        new CancelResult(1L, false))), secondResults);
    assertEquals(List.of(bid, ask, finalFill, cancel), ArchiveTestSupport.readAll(archiveDirectory),
        "Recovery and retries must not append; only the two new requests may extend the log");
  }

  @Test
  @Timeout(30)
  void rejectsDuplicateOrderAndProcessesNextCommand(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    ArchiveTestSupport.record(archiveDirectory,
        List.of(new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L))));

    // Recover bid 1, reject an attempt to change its price and size, then match a valid ask.
    List<CommandResult> results = runServerSession(tempDir, archiveDirectory,
        List.of(new PlaceOrder(1L, Side.BID, 200L, 2L), new PlaceOrder(2L, Side.ASK, 99L, 4L)), false);
    assertEquals(List.of(new RejectResult(1L, RejectReason.DUPLICATE_ORDER_ID),
        new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)), results);

    // Record the rejected request too, so its response can be reconstructed after restart.
    assertEquals(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(1L, Side.BID, 200L, 2L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L)), recordedCommands(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void servesAnotherClientAfterFirstClientDisconnects(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");

    // Each inner list uses a new client connection, but both talk to the same server JVM.
    List<List<CommandResult>> results = runServerWithClients(tempDir, archiveDirectory,
        List.of(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L)),
            List.of(new PlaceOrder(3L, Side.ASK, 100L, 6L), new CancelOrder(1L))),
        false);

    assertEquals(
        List.of(new PlaceResult(1L, List.of(), 10L), new PlaceResult(2L, List.of(new Trade(2L, 1L, 100L, 4L)), 0L)),
        results.get(0));
    // The second client consumes the six lots left by the first; bid 1 is then gone.
    assertEquals(List.of(new PlaceResult(3L, List.of(new Trade(3L, 1L, 100L, 6L)), 0L), new CancelResult(1L, false)),
        results.get(1));
    assertEquals(List.of(new PlaceOrder(1L, Side.BID, 100L, 10L), new PlaceOrder(2L, Side.ASK, 99L, 4L),
        new PlaceOrder(3L, Side.ASK, 100L, 6L), new CancelOrder(1L)), recordedCommands(archiveDirectory));
  }

  @Test
  @Timeout(30)
  void shutsDownCleanlyWhileIdle(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");

    // No client ever connects: the server must still notice the shutdown request and close its driver.
    runServerWithClients(tempDir, archiveDirectory, List.of(), false);

    assertEquals(List.of(), ArchiveTestSupport.readAll(archiveDirectory));
  }

  @Test
  @Timeout(20)
  void shutsDownWhileAReplyHasNoSubscriber(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    Path serverLog = tempDir.resolve("server.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName(), archiveDirectory.toString()).redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();
    CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    try {
      awaitServerOutput(server, serverLog, "Server ready:");
      try (
          Aeron aeron = Aeron
              .connect(new Aeron.Context().aeronDirectoryName(driverDirectory.toString()).errorHandler(errors::add));
          Publication commands = aeron.addPublication("aeron:ipc", 1)) {
        // Publish a request without ever creating a reply subscription.
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        int length = new SbeRequestCodec().encode(request, buffer, 0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        while (commands.offer(buffer, 0, length) < 0) {
          assertTrue(System.nanoTime() - deadline < 0, "Test request could not be sent");
          idle.idle();
        }
        try (AeronArchive archive = ArchiveTestSupport.connect(driverDirectory)) {
          long id = ArchiveTestSupport.recordingId(archive);
          ArchiveTestSupport.await(() -> archive.getRecordingPosition(id) > 0);
        }
        assertTrue(errors.isEmpty(), errors::toString);
      }

      // SIGTERM must leave the loop without waiting for the five-second reply timeout.
      server.destroy();
      assertTrue(server.waitFor(3, TimeUnit.SECONDS), "Shutdown waited on an undeliverable reply");
      String output = Files.readString(serverLog);
      assertFalse(output.contains("Exception"), output);
      assertFalse(output.contains("Server shutdown exceeded"), output);
      assertFalse(Files.exists(driverDirectory), "Shutdown did not remove the Aeron driver directory\n" + output);
      assertEquals(List.of(request), ArchiveTestSupport.readAll(archiveDirectory));
    } finally {
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  @ParameterizedTest(name = "recording fails: {0}")
  @ValueSource(booleans = {false, true})
  @Timeout(15)
  void reportsAgentFailureAndClosesResources(boolean failRecording, @TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    Path serverLog = tempDir.resolve("server.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName(), archiveDirectory.toString(), "--quiet").redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();
    CommandRequest request = new CommandRequest(new UUID(0L, 1L), new PlaceOrder(1L, Side.BID, 100L, 10L));
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    try {
      awaitServerOutput(server, serverLog, "Server ready:");
      if (failRecording) {
        // Stop the real recording after startup; the next command must fail before engine mutation.
        try (AeronArchive archive = ArchiveTestSupport.connect(driverDirectory)) {
          long id = ArchiveTestSupport.recordingId(archive);
          archive.tryStopRecordingByIdentity(id);
          ArchiveTestSupport.await(() -> archive.getStopPosition(id) != AeronArchive.NULL_POSITION);
        }
      }
      try (
          Aeron aeron = Aeron
              .connect(new Aeron.Context().aeronDirectoryName(driverDirectory.toString()).errorHandler(errors::add));
          Publication commands = aeron.addPublication("aeron:ipc", 1)) {
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        int length = new SbeRequestCodec().encode(request, buffer, 0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        while (commands.offer(buffer, 0, length) < 0) {
          assertTrue(server.isAlive(), "Server exited before the test could send its request");
          assertTrue(System.nanoTime() - deadline < 0, "Test request could not be sent");
          idle.idle();
        }
        assertTrue(errors.isEmpty(), errors::toString);

        // No reply subscriber connects: a successful recording write is followed by the reply-send timeout.
        assertTrue(server.waitFor(8, TimeUnit.SECONDS),
            "Agent failure did not stop the server\n" + Files.readString(serverLog));
        assertTrue(server.exitValue() != 0, "Agent failure was reported as a successful process exit");
      }
      String output = Files.readString(serverLog);
      assertTrue(output.contains("IllegalStateException: Engine agent failed"), output);
      String expectedCause = failRecording
          ? "Caused by: java.lang.IllegalStateException: Request recording stopped unexpectedly:"
          : "Caused by: java.lang.IllegalStateException: Timed out sending reply";
      assertTrue(output.contains(expectedCause),
          "The original agent failure must remain the exception cause\n" + output);
      assertFalse(output.contains("Server shutdown exceeded"), output);
      assertFalse(output.contains("failed to close"), output);
      assertFalse(output.contains("Result:"), "An undelivered response must not be logged as sent\n" + output);
      assertFalse(Files.exists(driverDirectory), "The runner and driver were not closed after failure\n" + output);
      if (failRecording) {
        assertEquals(List.of(), ArchiveTestSupport.readAll(archiveDirectory),
            "A failed recording must not accept the request");
      } else {
        assertEquals(List.of(request), ArchiveTestSupport.readAll(archiveDirectory),
            "A reply failure must preserve the single recorded command for recovery");
      }
    } finally {
      if (server.isAlive()) {
        server.destroyForcibly();
        server.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  @Timeout(35)
  void recoversAfterAbruptProcessDeath(@TempDir Path tempDir) throws Exception {
    Path archiveDirectory = tempDir.resolve("archive");
    Path serverLog = tempDir.resolve("crash.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    CommandRequest bid = new CommandRequest(new UUID(0, 1), new PlaceOrder(1, Side.BID, 100, 10));
    CommandRequest ask = new CommandRequest(new UUID(0, 2), new PlaceOrder(2, Side.ASK, 99, 4));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + tempDir, "-cp", classpath,
        AeronEngineServer.class.getName(), archiveDirectory.toString()).redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();
    try {
      awaitServerOutput(server, serverLog, "Server ready:");
      exchangeRequests(server, serverLog, driverDirectory, List.of(bid, ask), false, false);
    } finally {
      server.destroyForcibly();
      assertTrue(server.waitFor(3, TimeUnit.SECONDS));
    }
    // Archive's mark file deliberately rejects another owner until its ten-second heartbeat lease expires.
    Thread.sleep(11_000);
    CommandRequest fill = new CommandRequest(new UUID(0, 3), new PlaceOrder(3, Side.ASK, 100, 6));
    assertEquals(
        List.of(List.of(new PlaceResult(2, List.of(new Trade(2, 1, 100, 4)), 0),
            new PlaceResult(3, List.of(new Trade(3, 1, 100, 6)), 0))),
        runServerWithRequests(tempDir.resolve("restart"), archiveDirectory, List.of(List.of(ask, fill)), false, false));
    assertEquals(List.of(bid, ask, fill), ArchiveTestSupport.readAll(archiveDirectory));
  }

  private static List<EngineCommand> recordedCommands(Path path) throws IOException {
    return ArchiveTestSupport.readAll(path).stream().map(CommandRequest::command).toList();
  }

  private static List<CommandResult> runServerSession(Path tempDir, Path archiveDirectory, List<EngineCommand> orders,
      boolean delayFinalReply) throws Exception {
    return runServerWithClients(tempDir, archiveDirectory, List.of(orders), delayFinalReply).getFirst();
  }

  private static List<List<CommandResult>> runServerWithClients(Path tempDir, Path archiveDirectory,
      List<List<EngineCommand>> clientSessions, boolean delayFinalReply) throws Exception {
    return runServerWithClients(tempDir, archiveDirectory, clientSessions, delayFinalReply, false);
  }

  private static List<List<CommandResult>> runServerWithClients(Path tempDir, Path archiveDirectory,
      List<List<EngineCommand>> clientSessions, boolean delayFinalReply, boolean fragmentRequests) throws Exception {
    List<List<CommandRequest>> requestSessions = clientSessions.stream()
        .map(orders -> orders.stream().map(order -> new CommandRequest(UUID.randomUUID(), order)).toList()).toList();
    return runServerWithRequests(tempDir, archiveDirectory, requestSessions, delayFinalReply, fragmentRequests);
  }

  private static List<List<CommandResult>> runServerWithRequests(Path tempDir, Path archiveDirectory,
      List<List<CommandRequest>> clientSessions, boolean delayFinalReply, boolean fragmentRequests) throws Exception {
    Files.createDirectories(tempDir);
    Path serverLog = tempDir.resolve("server.log");
    Path driverDirectory = tempDir.resolve("exchange-lab-aeron");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process server = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Daeron.ipc.mtu.length=" + (fragmentRequests ? 64 : IPC_MTU_LENGTH), "-Djava.io.tmpdir=" + tempDir, "-cp",
        classpath, AeronEngineServer.class.getName(), archiveDirectory.toString()).redirectErrorStream(true)
        .redirectOutput(serverLog.toFile()).start();

    try {
      awaitServerOutput(server, serverLog, "Server ready:");
      assertFalse(server.waitFor(250, TimeUnit.MILLISECONDS), "Server exited while waiting for a client");

      List<List<CommandResult>> results = new ArrayList<>();
      for (List<CommandRequest> requests : clientSessions) {
        results.add(exchangeRequests(server, serverLog, driverDirectory, requests, delayFinalReply, fragmentRequests));
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

  private static List<CommandResult> exchangeRequests(Process server, Path serverLog, Path driverDirectory,
      List<CommandRequest> requests, boolean delayFinalReply, boolean fragmentRequests) throws Exception {
    List<CommandResult> received = new ArrayList<>();
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    try (
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driverDirectory.toString()).errorHandler(errors::add));
        Publication commands = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {
      SbeRequestCodec requestCodec = new SbeRequestCodec();
      CommandResponseCodec responseCodec = new CommandResponseCodec();
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
      List<CommandResponse> responses = new ArrayList<>();
      FragmentAssembler assembler = new FragmentAssembler((replyBuffer, offset, length, header) -> responses
          .add(responseCodec.decode(replyBuffer.getStringAscii(offset))));

      for (int i = 0; i < requests.size(); i++) {
        CommandRequest request = requests.get(i);
        int length = requestCodec.encode(request, buffer, 0);
        if (fragmentRequests) {
          // A 64-byte MTU leaves a 32-byte payload, so the valid 49-byte place request fragments.
          assertTrue(length > commands.maxPayloadLength(), "The test must send a fragmented request");
          assertTrue(length <= commands.maxMessageLength(), "The fixture must fit within Aeron's message limit");
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (commands.offer(buffer, 0, length) < 0) {
          assertTrue(System.nanoTime() - deadline < 0, "Timed out sending test command");
          idle.idle();
        }

        if (delayFinalReply && i == requests.size() - 1) {
          // Wait until the final reply is queued, then deliberately delay consuming it.
          awaitServerOutput(server, serverLog, "Result: PlaceResult[orderId=2,");
          assertFalse(server.waitFor(750, TimeUnit.MILLISECONDS),
              "Server stopped its driver before the client consumed the final reply");
        }

        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (received.size() <= i) {
          int fragments = replies.poll(assembler, 1);
          // Only a complete response carrying this request's UUID can satisfy the wait.
          for (CommandResponse response : responses) {
            if (request.requestId().equals(response.requestId())) {
              received.add(response.result());
            }
          }
          responses.clear();
          assertTrue(server.isAlive(), "Server exited before replying\n" + Files.readString(serverLog));
          assertTrue(errors.isEmpty(), errors::toString);
          assertTrue(System.nanoTime() - deadline < 0, "Timed out receiving test reply");
          idle.idle(fragments);
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
