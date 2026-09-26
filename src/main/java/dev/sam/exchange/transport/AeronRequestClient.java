package dev.sam.exchange.transport;

import java.util.ArrayList;
import java.util.List;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronRequestClient {
  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final SbeResponseCodec responseCodec = new SbeResponseCodec();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final IdleStrategy idle = new SleepingIdleStrategy();
  private final List<CommandResponse> receivedResponses = new ArrayList<>();
  private final FragmentHandler replyHandler = (replyBuffer, offset, length, header) -> {
    CommandResponse response = responseCodec.decode(replyBuffer, offset, length);
    receivedResponses.add(response);
  };
  private final FragmentAssembler replyAssembler = new FragmentAssembler(replyHandler);

  private final Publication publication;
  private final Subscription replies;
  private final long timeoutNanos;
  private final int maxAttempts;

  public AeronRequestClient(Publication publication, Subscription replies) {
    this(publication, replies, ClientConfig.defaults());
  }

  public AeronRequestClient(Publication publication, Subscription replies, ClientConfig config) {
    this.publication = publication;
    this.replies = replies;
    this.timeoutNanos = config.timeout().toNanos();
    this.maxAttempts = config.maxAttempts();
  }

  public CommandResult send(CommandRequest request) {

    int messageLength = requestCodec.encode(request, buffer, 0);
    List<CommandResult> matchingResults = new ArrayList<>(1);
    for (int attempt = 1; attempt <= this.maxAttempts && matchingResults.isEmpty(); attempt++) {

      // Each send and reply wait gets its own configured timeout.
      long deadline = System.nanoTime() + this.timeoutNanos;
      long offerResult;
      // A later attempt means an earlier send was accepted and may already have been processed.
      String failureContext = attempt > 1 ? "; request " + request.requestId() + "; outcome unknown" : "";

      // Retry temporary offer failures until the send deadline.
      // A successful offer queues bytes; the server may not have processed the order yet.
      while ((offerResult = publication.offer(buffer, 0, messageLength)) < 0) {
        // These failures cannot be resolved by retrying.
        if (offerResult == Publication.CLOSED) {
          throw new IllegalStateException("Publication is closed" + failureContext);
        }
        if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
          throw new IllegalStateException("Publication reached its maximum position" + failureContext);
        }
        if (System.nanoTime() - deadline >= 0) {
          throw new IllegalStateException(
              "Timed out sending order; last offer result: " + offerResult + failureContext);
        }
        // No subscriber, back pressure, and administrative actions are retryable.
        idle.idle();
      }

      // Receiving a fragment is not enough: wait for the matching UUID.
      // Unrelated replies do not reset this deadline.
      deadline = System.nanoTime() + this.timeoutNanos;

      while (matchingResults.isEmpty()) {
        int fragments = replies.poll(replyAssembler, 1);

        // Here, both the current request and complete responses are available.
        for (CommandResponse response : receivedResponses) {
          if (request.requestId().equals(response.requestId())) {
            matchingResults.add(response.result());
          }
        }
        receivedResponses.clear();

        if (matchingResults.isEmpty() && System.nanoTime() - deadline >= 0) {
          break;
        }

        idle.idle(fragments);
      }
    }
    if (matchingResults.isEmpty()) {
      throw new IllegalStateException(
          "No reply after " + this.maxAttempts + " attempts for request " + request.requestId() + "; outcome unknown");
    }
    return matchingResults.getFirst();
  }
}
