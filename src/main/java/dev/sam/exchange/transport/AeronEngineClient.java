package dev.sam.exchange.transport;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.persistence.CommandCodec;
import io.aeron.Aeron;
import io.aeron.Publication;

public class AeronEngineClient {
  public static void main(String[] args) {
    // 1. Find the driver that the server started.
    // This directory must match the server's directory.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // 2. These are the commands this client wants the server to process.
    List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L));

    // 3. Connect to the existing driver and open a sending endpoint.
    // The channel and stream ID match the server's subscription.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1)) {

      CommandCodec codec = new CommandCodec();
      IdleStrategy idle = new SleepingIdleStrategy();

      // Reuse this buffer for each order.
      UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

      for (EngineCommand order : orders) {
        // 4. Encode the order with the codec.
        String orderEncoding = codec.encode(order);

        // 5. Write the string into the buffer.
        // Save the byte count returned by putStringAscii().
        int messageLength = buffer.putStringAscii(0, orderEncoding);

        // 6. Offer those bytes to the publication.
        // Retry temporary failures, idle between attempts,
        // and stop with an exception after five seconds.
        // CLOSED and MAX_POSITION_EXCEEDED should fail immediately.
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

        // 7. After a successful offer, print which order was offered.
        System.out.println("Successfully sent order; offer result: " + offerResult);
      }
    }
  }
}
