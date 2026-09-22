package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException, InterruptedException {

    String journalArgument = null;
    boolean quiet = false;
    for (String arg : args) {
      if ("--quiet".equals(arg)) {
        quiet = true;
      } else if (journalArgument == null && !arg.startsWith("--")) {
        journalArgument = arg;
      } else {
        throw new IllegalArgumentException("Usage: AeronEngineServer [journalPath] [--quiet]");
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

    // Recover the existing journal before accepting new requests.
    Path journalPath = journalArgument != null ? Path.of(journalArgument) : Path.of("data", "requests.journal");
    Path parent = journalPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    if (Files.notExists(journalPath)) {
      Files.createFile(journalPath);
    }
    RequestProcessor requestProcessor = RequestProcessor.recover(journalPath);

    // Both processes use this directory to connect to the same media driver.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The server owns the driver; resources close in reverse order on exit.
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectory).dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2);
        // Busy spinning keeps polling for requests; budget a dedicated core for this runner.
        AgentRunner runner = new AgentRunner(new BusySpinIdleStrategy(), errorHandler, null,
            new AeronEngineAgent(subscription, replies, requestProcessor, !quiet))) {

      AgentRunner.startOnThread(runner);

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Journal: " + journalPath);
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
