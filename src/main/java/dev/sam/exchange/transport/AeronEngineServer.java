package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException {

    AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    Thread serverThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Ask the main thread to leave its processing loop.
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
    Path journalPath = args.length > 0 ? Path.of(args[0]) : Path.of("data", "requests.journal");
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
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {

      AeronEngineAgent agent = new AeronEngineAgent(subscription, replies, requestProcessor);
      IdleStrategy idle = new SleepingIdleStrategy();

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Journal: " + journalPath);
      System.out.println("Waiting for requests.");

      while (!shutdownRequested.get()) {
        int workCount = agent.doWork();
        idle.idle(workCount);
      }
    }
  }
}
