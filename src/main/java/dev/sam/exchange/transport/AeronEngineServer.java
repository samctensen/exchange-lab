package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import dev.sam.exchange.JournaledEngine;
import dev.sam.exchange.engine.CommandResult;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

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
    Path journalPath = args.length > 0 ? Path.of(args[0]) : Path.of("data", "commands.journal");
    Path parent = journalPath.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    if (Files.notExists(journalPath)) {
      Files.createFile(journalPath);
    }
    JournaledEngine journaledEngine = JournaledEngine.recover(journalPath);

    // Both processes use this directory to connect to the same media driver.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The server owns the driver; resources close in reverse order on exit.
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectory).dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
        Publication replies = aeron.addPublication("aeron:ipc", 2)) {

      IdleStrategy idle = new SleepingIdleStrategy();
      CommandRequestCodec requestCodec = new CommandRequestCodec();

      // Keep each command together with the request ID that must accompany its reply.
      List<CommandRequest> receivedRequests = new ArrayList<>();

      // The assembler invokes this handler on the main thread for each complete message.
      // getStringAscii(offset) reads the string-length prefix followed by the text.
      FragmentHandler handler = (buffer, offset, length, header) -> {
        String encoded = buffer.getStringAscii(offset);
        CommandRequest request = requestCodec.decode(encoded);
        receivedRequests.add(request);
      };

      FragmentAssembler assembler = new FragmentAssembler(handler);

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Journal: " + journalPath);
      System.out.println("Waiting for requests.");

      // Stream 1 receives requests; stream 2 publishes correlated responses.
      CommandResponseCodec responseCodec = new CommandResponseCodec();
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);

      // Process requests until the shutdown hook asks this thread to stop.
      while (!shutdownRequested.get()) {
        int fragments = subscription.poll(assembler, 1);
        idle.idle(fragments);

        if (receivedRequests.isEmpty()) {
          continue;
        }
        // Process once, outside the callback so journal IOExceptions can propagate.
        CommandRequest request = receivedRequests.removeFirst();
        CommandResult result = journaledEngine.process(request.command());

        CommandResponse response = new CommandResponse(request.requestId(), result);
        String responseEncoding = responseCodec.encode(response);
        // Send exactly the bytes written, including the string-length prefix.
        int responseMessageLength = buffer.putStringAscii(0, responseEncoding);
        long offerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long offerResult;

        // Retry sending the same result without processing the command again.
        // A successful offer queues bytes; it does not confirm the client received them.
        while ((offerResult = replies.offer(buffer, 0, responseMessageLength)) < 0) {
          // These failures cannot be resolved by retrying.
          if (offerResult == Publication.CLOSED) {
            throw new IllegalStateException("Publication is closed");
          }
          if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("Publication reached its maximum position");
          }
          if (System.nanoTime() - offerDeadline >= 0) {
            throw new IllegalStateException("Timed out sending reply; last offer result: " + offerResult);
          }

          idle.idle();
        }

        System.out.println("Result: " + result);
      }
    }
  }
}
