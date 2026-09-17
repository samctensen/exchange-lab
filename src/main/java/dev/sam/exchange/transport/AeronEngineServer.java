package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import dev.sam.exchange.JournaledEngine;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException {
    // Both commands share one engine and a fresh journal for this demo run.
    Path journalPath = Files.createTempFile("exchange-aeron-", ".journal");
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
      CommandCodec codec = new CommandCodec();

      // Hold received commands until the processing loop consumes them.
      List<EngineCommand> receivedCommands = new ArrayList<>();

      // poll() invokes this handler on the main thread. Each demo command fits in one fragment.
      // getStringAscii(offset) reads the string-length prefix followed by the text.
      FragmentHandler handler = (buffer, offset, length, header) -> {
        String encoded = buffer.getStringAscii(offset);
        EngineCommand command = codec.decode(encoded);
        receivedCommands.add(command);
      };

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Journal: " + journalPath);
      System.out.println("Waiting for two commands; each has a 60-second receive timeout.");

      // Stream 1 receives commands; stream 2 publishes their results.
      CommandResultCodec resultCodec = new CommandResultCodec();
      UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

      // This demo handles two commands, then exits.
      for (int processed = 0; processed < 2; processed++) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        // Poll one fragment at a time; idle when no command is available.
        while (subscription.poll(handler, 1) == 0) {
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out waiting for command " + (processed + 1));
          }
          idle.idle();
        }

        // Process once, outside the callback so journal IOExceptions can propagate.
        EngineCommand command = receivedCommands.removeFirst();
        CommandResult result = journaledEngine.process(command);

        String resultEncoding = resultCodec.encode(result);
        // Send exactly the bytes written, including the string-length prefix.
        int resultMessageLength = buffer.putStringAscii(0, resultEncoding);
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long offerResult;

        // Retry sending the same result without processing the command again.
        // A successful offer queues bytes; it does not confirm the client received them.
        while ((offerResult = replies.offer(buffer, 0, resultMessageLength)) < 0) {
          // These failures cannot be resolved by retrying.
          if (offerResult == Publication.CLOSED) {
            throw new IllegalStateException("Publication is closed");
          }
          if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("Publication reached its maximum position");
          }
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out sending reply; last offer result: " + offerResult);
          }

          idle.idle();
        }

        System.out.println("Result: " + result);
      }
    }
  }
}
