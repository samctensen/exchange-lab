package dev.sam.exchange.transport;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
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

    // Send requests on stream 1 and receive correlated responses on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {

      CommandRequestCodec requestCodec = new CommandRequestCodec();
      IdleStrategy idle = new SleepingIdleStrategy();

      // Reuse this buffer for the small commands in this demo.
      UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

      CommandResponseCodec responseCodec = new CommandResponseCodec();

      for (EngineCommand order : orders) {
        CommandRequest request = new CommandRequest(UUID.randomUUID(), order);
        String requestEncoding = requestCodec.encode(request);

        // poll() invokes the handler on this thread, so this list needs no synchronization.
        // A reply for an older request or another client must not complete the current request.
        List<CommandResult> matchingResults = new ArrayList<>(1);
        FragmentHandler replyHandler = (replyBuffer, offset, length, header) -> {
          CommandResponse response = responseCodec.decode(replyBuffer.getStringAscii(offset));
          if (request.requestId().equals(response.requestId())) {
            matchingResults.add(response.result());
          }
        };

        // Send exactly the bytes written, including the string-length prefix.
        int messageLength = buffer.putStringAscii(0, requestEncoding);

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

        // Receiving a fragment is not enough: wait for the matching UUID.
        // Unrelated replies do not reset this deadline.
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (matchingResults.isEmpty()) {
          int fragments = replies.poll(replyHandler, 1);
          if (matchingResults.isEmpty() && System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("Timed out waiting for reply to request " + request.requestId());
          }
          idle.idle(fragments);
        }

        System.out.println(matchingResults.getFirst());
        System.out.println("Successfully sent order; offer result: " + offerResult);
      }
    }
  }
}
