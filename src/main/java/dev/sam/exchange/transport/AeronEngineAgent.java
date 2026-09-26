package dev.sam.exchange.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import dev.sam.exchange.persistence.RequestLog;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineAgent implements Agent {
  private final Subscription requests;
  private final Publication replies;
  private final RequestStateMachine processor;
  private final RequestLog requestLog;
  private final NanoClock clock;
  private final boolean logResults;

  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
  private CommandRequest pendingRequest;
  private long pendingLogPosition = -1;
  private long logDeadlineNanos;
  private CommandResponse pendingResponse;
  private int pendingResponseLength;
  private long replyDeadlineNanos;

  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final CommandResponseCodec responseCodec = new CommandResponseCodec();
  private final List<CommandRequest> receivedRequests = new ArrayList<>();
  private final FragmentHandler handler = (buffer, offset, length, header) -> {
    CommandRequest request = requestCodec.decode(buffer, offset, length);
    receivedRequests.add(request);
  };
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final FragmentAssembler assembler = new FragmentAssembler(handler);

  public AeronEngineAgent(Subscription requests, Publication replies, RequestStateMachine processor,
      RequestLog requestLog) {
    this(requests, replies, processor, requestLog, true);
  }

  public AeronEngineAgent(Subscription requests, Publication replies, RequestStateMachine processor,
      RequestLog requestLog, boolean logResults) {
    this(requests, replies, processor, requestLog, logResults, SystemNanoClock.INSTANCE);
  }

  AeronEngineAgent(Subscription requests, Publication replies, RequestStateMachine processor, RequestLog requestLog,
      boolean logResults, NanoClock clock) {
    this.requests = requests;
    this.replies = replies;
    this.processor = processor;
    this.requestLog = requestLog;
    this.logResults = logResults;
    this.clock = clock;
  }

  @Override
  public int doWork() {
    // Finish delivery before admitting another request. AgentRunner owns idling between passes.
    if (pendingResponse != null)
      return offerPendingReply();

    int work = 0;
    if (pendingRequest == null) {
      work = requests.poll(assembler, 1);
      // FragmentAssembler enqueues only complete requests.
      if (receivedRequests.isEmpty())
        return work;
      pendingRequest = receivedRequests.removeFirst();
      if (processor.hasProcessed(pendingRequest.requestId())) {
        // The original request was already recorded. Retries and UUID conflicts need no new log entry.
        prepareReply();
        return work + 1 + offerPendingReply();
      }
      pendingLogPosition = -1;
      logDeadlineNanos = clock.nanoTime() + TIMEOUT_NS;
    }

    if (pendingLogPosition < 0) {
      long position = requestLog.offer(pendingRequest);
      if (position < 0) {
        checkLogDeadline("offering request; last offer result: " + position);
        return work;
      }
      pendingLogPosition = position;
      // Once accepted, never offer this request again. Wait for this exact end position.
      logDeadlineNanos = clock.nanoTime() + TIMEOUT_NS;
      work++;
    }

    if (!requestLog.isRecorded(pendingLogPosition)) {
      checkLogDeadline("request recording at position " + pendingLogPosition);
      return work;
    }

    // Archive has written the request. Only now may it mutate the engine or produce a reply.
    prepareReply();
    return work + 1 + offerPendingReply();
  }

  private void prepareReply() {
    pendingResponse = processor.process(pendingRequest);
    pendingRequest = null;
    pendingResponseLength = buffer.putStringAscii(0, responseCodec.encode(pendingResponse));
    replyDeadlineNanos = clock.nanoTime() + TIMEOUT_NS;
  }

  private void checkLogDeadline(String operation) {
    if (clock.nanoTime() - logDeadlineNanos >= 0) {
      throw new IllegalStateException("Timed out " + operation);
    }
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
      if (logResults) {
        System.out.println("Result: " + pendingResponse.result());
      }
      pendingResponse = null;
      return 1;
    }

    if (offerResult == Publication.CLOSED) {
      throw new IllegalStateException("Publication is closed");
    }

    if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
      throw new IllegalStateException("Publication reached its maximum position");
    }

    if (clock.nanoTime() - replyDeadlineNanos >= 0) {
      throw new IllegalStateException("Timed out sending reply; last offer result: " + offerResult);
    }

    return 0;
  }
}
