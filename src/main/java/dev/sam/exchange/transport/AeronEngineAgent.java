package dev.sam.exchange.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineAgent implements Agent {
  private final Subscription requests;
  private final Publication replies;
  private final RequestProcessor processor;

  private CommandResponse pendingResponse;
  private int pendingResponseLength;
  private long replyDeadlineNanos;

  private final CommandRequestCodec requestCodec = new CommandRequestCodec();
  private final CommandResponseCodec responseCodec = new CommandResponseCodec();
  private final List<CommandRequest> receivedRequests = new ArrayList<>();
  private final FragmentHandler handler = (buffer, offset, length, header) -> {
    String encoded = buffer.getStringAscii(offset);
    CommandRequest request = requestCodec.decode(encoded);
    receivedRequests.add(request);
  };
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final FragmentAssembler assembler = new FragmentAssembler(handler);

  public AeronEngineAgent(Subscription requests, Publication replies, RequestProcessor processor) {
    this.requests = requests;
    this.replies = replies;
    this.processor = processor;
  }

  @Override
  public int doWork() throws IOException {
    // Finish delivering the previous response before taking another request.
    if (pendingResponse != null) {
      return offerPendingReply();
    }

    int fragments = requests.poll(assembler, 1);

    // The assembler adds to receivedRequests only when a whole message is ready.
    if (receivedRequests.isEmpty()) {
      return fragments;
    }

    CommandRequest request = receivedRequests.removeFirst();
    pendingResponse = processor.process(request);

    // Prepare once. Later calls retry these same bytes with this same deadline.
    pendingResponseLength = buffer.putStringAscii(0, responseCodec.encode(pendingResponse));
    replyDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

    int repliesSent = offerPendingReply();

    // Count fragments read, one command processed, and any reply sent.
    return fragments + 1 + repliesSent;
  }

  @Override
  public String roleName() {
    return "exchange-engine";
  }

  private int offerPendingReply() {
    if (pendingResponse == null) {
      return 0;
    }

    long offerResult = replies.offer(buffer, 0, pendingResponseLength);

    if (offerResult >= 0) {
      System.out.println("Result: " + pendingResponse.result());
      pendingResponse = null;
      return 1;
    }

    if (offerResult == Publication.CLOSED) {
      throw new IllegalStateException("Publication is closed");
    }

    if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
      throw new IllegalStateException("Publication reached its maximum position");
    }

    if (System.nanoTime() - replyDeadlineNanos >= 0) {
      throw new IllegalStateException("Timed out sending reply; last offer result: " + offerResult);
    }

    return 0;
  }
}
