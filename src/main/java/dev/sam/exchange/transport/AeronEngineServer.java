package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import dev.sam.exchange.JournaledEngine;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException {
    // 1. Each demo run starts with an empty journal and a fresh engine.
    // Keep this one engine alive for both commands so the second can match the first.
    Path journalPath = Files.createTempFile("exchange-aeron-", ".journal");
    JournaledEngine journaledEngine = JournaledEngine.recover(journalPath);

    // 2. A separate client needs a known directory to find this server's driver.
    // Both processes must use this directory, channel "aeron:ipc", and stream ID 1.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // 3. The server owns the driver. A client will connect to it and publish orders.
    // Closing the try block closes these resources in reverse order and deletes the Aeron directory.
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectory).dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1)) {

      IdleStrategy idle = new SleepingIdleStrategy();
      CommandCodec codec = new CommandCodec();

      // 4. This list starts EMPTY. Commands are added only when the subscription receives them.
      // The server does not create a hardcoded list of bid and ask orders.
      List<EngineCommand> receivedCommands = new ArrayList<>();

      // poll() calls this handler on the main thread when a message fragment is available.
      // Our small command messages fit in one fragment. getStringAscii reads their length prefix.
      FragmentHandler handler = (buffer, offset, length, header) -> {
        String encoded = buffer.getStringAscii(offset);
        EngineCommand command = codec.decode(encoded);
        receivedCommands.add(command);
      };

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Journal: " + journalPath);
      System.out.println("Waiting for two commands; each has a 60-second receive timeout.");

      // 5. Count commands as they arrive. There is no local orders list to loop over.
      // This learning version processes two commands and then shuts down normally.
      for (int processed = 0; processed < 2; processed++) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        // Handle at most one fragment per poll. Zero means nothing arrived yet.
        // Briefly park between attempts so waiting does not consume a CPU core.
        while (subscription.poll(handler, 1) == 0) {
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out waiting for command " + (processed + 1));
          }
          idle.idle();
        }

        // 6. Consume the received command exactly once, then validate, journal, and match it.
        // Processing outside the callback lets an IOException leave main and close the resources.
        EngineCommand command = receivedCommands.removeFirst();
        CommandResult result = journaledEngine.process(command);
        System.out.println("Result: " + result);
      }
    }
  }
}
