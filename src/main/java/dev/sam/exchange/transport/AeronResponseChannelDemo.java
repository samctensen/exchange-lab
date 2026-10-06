package dev.sam.exchange.transport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

/**
 * Two logical clients and one echo server, all in this JVM, using IPC response channels.
 * Both clients use the same stream IDs, but Aeron delivers each reply only to its intended client.
 * See https://github.com/aeron-io/aeron/wiki/Response-Channels for the underlying channel configuration.
 */
public class AeronResponseChannelDemo {
  private static final int REQUEST_STREAM_ID = 1;
  private static final int RESPONSE_STREAM_ID = 2;
  private static final String RESPONSE_CHANNEL = "aeron:ipc?control-mode=response";

  public static void main(String[] args) {
    DemoResult result = run();
    System.out.println("Client A received: " + result.clientAReplies());
    System.out.println("Client B received: " + result.clientBReplies());

    if (!result.clientAReplies().equals(List.of("A")) || !result.clientBReplies().equals(List.of("B"))) {
      throw new IllegalStateException("A client received an unexpected reply: " + result);
    }
    System.out.println("Reply isolation verified: each client received only its own echo.");
  }

  // The integration test runs this same transport flow. This demo has exactly two clients,
  // each sending one small message; the phases below make the connection setup easy to follow.
  static DemoResult run() {
    // launchEmbedded chooses a unique directory, so this can run alongside the exchange server.
    // Resource declarations close in reverse order, and the driver deletes its directory on exit.
    try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription serverRequests = aeron.addSubscription("aeron:ipc", REQUEST_STREAM_ID);
        // Step 1: each client creates its own response subscription FIRST.
        Subscription clientAReplies = aeron.addSubscription(RESPONSE_CHANNEL, RESPONSE_STREAM_ID);
        Subscription clientBReplies = aeron.addSubscription(RESPONSE_CHANNEL, RESPONSE_STREAM_ID);
        // Step 2: link each request publication to that client's response subscription.
        // registrationId identifies the subscription within this running driver, not an order/request UUID.
        Publication clientARequests = aeron
            .addPublication("aeron:ipc?response-correlation-id=" + clientAReplies.registrationId(), REQUEST_STREAM_ID);
        Publication clientBRequests = aeron.addPublication(
            "aeron:ipc?response-correlation-id=" + clientBReplies.registrationId(), REQUEST_STREAM_ID)) {

      await(() -> serverRequests.imageCount() == 2 && clientARequests.isConnected() && clientBRequests.isConnected(),
          "waiting for both request connections");

      // An Image is the receiving view of one publication session. The shared subscription
      // contains one image per client here. Their discovery order does not tell us which is A or B.
      Image firstRequestImage = serverRequests.imageAtIndex(0);
      Image secondRequestImage = serverRequests.imageAtIndex(1);

      // Step 3: the SERVER uses image.correlationId(), not the client's subscription registrationId.
      // Aeron follows the connection association to find the correct client's response subscription.
      // Create publications on this main thread, outside Aeron's image-notification callbacks.
      try (Publication firstReply = responsePublication(aeron, firstRequestImage);
          Publication secondReply = responsePublication(aeron, secondRequestImage)) {
        await(() -> firstReply.isConnected() && secondReply.isConnected() && clientAReplies.isConnected()
            && clientBReplies.isConnected(), "waiting for both response connections");

        send(clientARequests, "A");
        send(clientBRequests, "B");

        // Step 4: associate each received message with the reply publication for its SOURCE image.
        // Poll each image directly so this association is explicit. The message text never selects a route.
        List<PendingEcho> pending = new ArrayList<>();
        await(() -> {
          receive(firstRequestImage, firstReply, pending);
          receive(secondRequestImage, secondReply, pending);
          return pending.size() == 2;
        }, "receiving both requests");

        // Retry offers outside receive callbacks. Each echo retains its source image's reply route.
        for (PendingEcho echo : pending) {
          send(echo.replies(), echo.message());
        }

        // Step 5: both replies are now published. Poll ALL available replies so the result also
        // exposes accidental broadcasts or duplicates. Aeron filters by the associated session;
        // there is no application-side filtering of A/B here.
        List<String> receivedByA = new ArrayList<>();
        List<String> receivedByB = new ArrayList<>();
        await(() -> {
          clientAReplies.poll((buffer, offset, length, header) -> receivedByA.add(buffer.getStringAscii(offset)), 10);
          clientBReplies.poll((buffer, offset, length, header) -> receivedByB.add(buffer.getStringAscii(offset)), 10);
          return !receivedByA.isEmpty() && !receivedByB.isEmpty();
        }, "receiving both echoes");

        return new DemoResult(receivedByA, receivedByB);
      }
    }
  }

  private static Publication responsePublication(Aeron aeron, Image requestImage) {
    return aeron.addPublication(RESPONSE_CHANNEL + "|response-correlation-id=" + requestImage.correlationId(),
        RESPONSE_STREAM_ID);
  }

  private static void receive(Image requests, Publication replies, List<PendingEcho> pending) {
    requests.poll((buffer, offset, length, header) -> {
      // Copy the tiny message into a String: the receive buffer belongs to Aeron and must not
      // be retained after this callback. A/B each fits in one fragment, so no assembler is needed.
      pending.add(new PendingEcho(replies, buffer.getStringAscii(offset)));
    }, 1);
  }

  private static void send(Publication publication, String message) {
    UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
    // putStringAscii includes a four-byte length prefix; its return value is the complete byte count.
    int length = buffer.putStringAscii(0, message);
    await(() -> {
      long result = publication.offer(buffer, 0, length);
      if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
        throw new IllegalStateException("Cannot send echo message; offer result: " + result);
      }
      // Other negative results are temporary: retry until accepted or the deadline expires.
      return result >= 0;
    }, "sending message " + message);
  }

  private static void await(BooleanSupplier completed, String operation) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!completed.getAsBoolean()) {
      if (Thread.currentThread().isInterrupted()) {
        throw new IllegalStateException("Interrupted while " + operation);
      }
      if (System.nanoTime() - deadline >= 0) {
        throw new IllegalStateException("Timed out " + operation);
      }
      // A short sleep keeps this teaching demo from consuming a core while waiting.
      idle.idle();
    }
  }

  private record PendingEcho(Publication replies, String message) {
  }

  record DemoResult(List<String> clientAReplies, List<String> clientBReplies) {
    DemoResult {
      clientAReplies = List.copyOf(clientAReplies);
      clientBReplies = List.copyOf(clientBReplies);
    }
  }
}
