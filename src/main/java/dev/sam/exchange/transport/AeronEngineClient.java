package dev.sam.exchange.transport;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineClient {
  public static void main(String[] args) {
    // Connect to the media driver owned by the server.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The ask partially fills the resting bid.
    List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L));

    // Send commands on stream 1 and receive their results on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {

      CommandCodec codec = new CommandCodec();
      IdleStrategy idle = new SleepingIdleStrategy();

      // Reuse this buffer for the small commands in this demo.
      UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

      CommandResultCodec resultCodec = new CommandResultCodec();

      // poll() invokes this handler on the main thread. Each demo reply fits in one fragment.
      // Read the length-prefixed text from the offset supplied by Aeron.
      FragmentHandler replyHandler = (replyBuffer, offset, length, header) -> {
        String encoded = replyBuffer.getStringAscii(offset);
        CommandResult command = resultCodec.decode(encoded);
        System.out.println(command);
      };

      for (EngineCommand order : orders) {
        String orderEncoding = codec.encode(order);

        // Send exactly the bytes written, including the string-length prefix.
        int messageLength = buffer.putStringAscii(0, orderEncoding);

        // Sending and receiving each get their own five-second timeout.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long offerResult;

        // Retry temporary offer failures until the send deadline.
        // A successful offer queues bytes; the server may not have processed the order yet.
        while ((offerResult = publication.offer(buffer, 0, messageLength)) < 0) {
          // These failures cannot be resolved by retrying.
          if (offerResult == Publication.CLOSED) {
            throw new IllegalStateException("Publication is closed");
          }
          if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("Publication reached its maximum position");
          }
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out sending order; last offer result: " + offerResult);
          }
          // No subscriber, back pressure, and administrative actions are retryable.
          idle.idle();
        }

        // Wait for this command's reply before sending the next command.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (replies.poll(replyHandler, 1) == 0) {
          if (System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out waiting for reply ");
          }
          idle.idle();
        }

        System.out.println("Successfully sent order; offer result: " + offerResult);
      }
    }
  }
}
