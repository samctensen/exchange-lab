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
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

// Each order makes a round trip: Java object -> text -> Aeron bytes -> text -> Java object.
// Sending and receiving happen on the main thread; Aeron also does work in background threads.
public class AeronOrderDemo {
  public static void main(String[] args) throws IOException {

    Path journalPath = Files.createTempFile("exchange-aeron-", ".journal");
    JournaledEngine engine = JournaledEngine.recover(journalPath);
    List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L));

    for (CommandResult result : run(orders, engine)) {
      System.out.println("Result: " + result);
    }
    System.out.println("Journal: " + journalPath);
  }

  // Keep the transport flow shared by the runnable demo and its integration test.
  // The caller supplies the engine, so it can inspect the resulting book and journal.
  static List<CommandResult> run(List<EngineCommand> orders, JournaledEngine engine) throws IOException {
    List<CommandResult> results = new ArrayList<>();

    // Start the transport driver inside this JVM. By default, it creates a uniquely named Aeron directory.
    // Delete that directory on shutdown. Try-with-resources closes the resources in reverse order,
    // including when an exception occurs: publication, subscription, client, then driver.
    try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnShutdown(true));
        // Connect our application to this specific driver by sharing its directory name.
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        // IPC uses shared memory on this machine. The channel and stream ID must match at both ends.
        // Stream ID 1 is our chosen identifier for this stream of messages within the IPC channel.
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
        // The publication is the sending endpoint; the subscription above is the receiving endpoint.
        Publication publication = aeron.addPublication("aeron:ipc", 1)) {

      // Briefly park the main thread between unsuccessful attempts so it does not spin continuously.
      IdleStrategy idle = new SleepingIdleStrategy();
      CommandCodec codec = new CommandCodec();

      List<EngineCommand> receivedCommands = new ArrayList<>();

      // Define what to do with received data. Creating this lambda does not receive anything;
      // poll() below calls it on our main thread when a fragment is available.
      FragmentHandler handler = (receivedBuffer, offset, length, header) -> {
        // Aeron supplies the message start as offset; it need not be 0 in the received buffer.
        // Read the stored string-length prefix, then that many text bytes. The callback length
        // includes the prefix too, so it is not the string length. header contains Aeron metadata.
        String received = receivedBuffer.getStringAscii(offset);
        // Turn the transported text back into a command object before adding it to the command list.
        EngineCommand decoded = codec.decode(received);
        receivedCommands.add(decoded);
      };

      for (EngineCommand order : orders) {
        String orderEncoding = codec.encode(order);
        // Aeron sends bytes. Allocate 256 bytes outside the Java heap and wrap them with Agrona
        // methods for reading and writing values such as strings.
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        // Starting at index 0, write [4-byte string length][ASCII text].
        // The return value includes both parts: send exactly these bytes, not the entire 256-byte buffer.
        int messageLength = buffer.putStringAscii(0, orderEncoding);

        // Give sending five seconds. nanoTime measures elapsed time independently of wall-clock changes.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long offerResult;

        // Every condition check calls offer() again and saves its result. Negative means not accepted.
        // A positive result is the new stream position: the bytes are in the publication log buffer,
        // but our receiving handler has not necessarily processed them yet.
        while ((offerResult = publication.offer(buffer, 0, messageLength)) < 0) {
          // These two results are permanent failures for this publication, so retrying cannot help.
          if (offerResult == Publication.CLOSED) {
            throw new IllegalStateException("Publication is closed");
          }
          if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("Publication reached its maximum position");
          }
          // Once now reaches the deadline, stop waiting instead of hanging forever.
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out sending order; last offer result: " + offerResult);
          }
          // Retry temporary results: no connected subscriber, back pressure (no capacity yet),
          // or an administrative action such as rotating the log buffer.
          idle.idle();
        }

        // Start a fresh five-second deadline so time spent sending does not consume the receive budget.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

        // Process at most one available fragment per call. Our short order fits in one fragment.
        // A return value of 0 means nothing was handled yet, so idle and try again.
        // A positive return means the handler ran, so this order is ready for the engine.
        while (subscription.poll(handler, 1) == 0) {
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out receiving order");
          }
          idle.idle();
        }

        // Remove the received command so the next iteration cannot process it a second time.
        CommandResult result = engine.process(receivedCommands.removeFirst());
        results.add(result);
      }
    }
    // Preserve the order of results while keeping the returned list immutable.
    return List.copyOf(results);
  }
}
