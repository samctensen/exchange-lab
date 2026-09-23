package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.persistence.ArchiveRequestLog;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException, InterruptedException {

    String archiveArgument = null;
    boolean quiet = false;
    for (String arg : args) {
      if ("--quiet".equals(arg)) {
        quiet = true;
      } else if (archiveArgument == null && !arg.startsWith("--")) {
        archiveArgument = arg;
      } else {
        throw new IllegalArgumentException("Usage: AeronEngineServer [archiveDirectory] [--quiet]");
      }
    }

    AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    AtomicReference<Throwable> agentFailure = new AtomicReference<>();
    ErrorHandler errorHandler = error -> {
      agentFailure.compareAndSet(null, error);
      shutdownRequested.set(true);
      Thread.currentThread().interrupt();
    };
    Thread serverThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Ask the main thread to begin closing the server.
      shutdownRequested.set(true);
      try {
        // Give it time to finish and close the try-with-resources block.
        serverThread.join(10_000);
        if (serverThread.isAlive()) {
          System.err.println("Server shutdown exceeded 10 seconds");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "server-shutdown"));

    Path archiveDirectory = archiveArgument != null ? Path.of(archiveArgument) : Path.of("data", "archive");
    RequestStateMachine stateMachine = new RequestStateMachine(new MatchingEngine(new OrderBook()));

    // Both processes use this directory to connect to the same media driver.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The server owns the driver; resources close in reverse order on exit.
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(archiveDirectory, aeronDirectory);
        // Replay the saved log before creating the live request subscription, then extend that same recording.
        ArchiveRequestLog requestLog = ArchiveRequestLog.open(runtime.archive(), stateMachine::process);
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2);
        // Busy spinning keeps polling for requests; budget a dedicated core for this runner.
        AgentRunner runner = new AgentRunner(new BusySpinIdleStrategy(), errorHandler, null,
            new AeronEngineAgent(subscription, replies, stateMachine, requestLog, !quiet))) {

      AgentRunner.startOnThread(runner);

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Archive: " + archiveDirectory);
      System.out.println("Request recording: " + requestLog.recordingId());
      System.out.println("Waiting for requests.");

      // The runner processes requests. Main waits here to keep resources open.
      while (!shutdownRequested.get() && !runner.isClosed()) {
        Thread.sleep(10);
      }
    }
    Throwable failure = agentFailure.get();
    if (failure != null) {
      throw new IllegalStateException("Engine agent failed", failure);
    }
  }
}
