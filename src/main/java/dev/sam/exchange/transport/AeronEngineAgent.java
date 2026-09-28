package dev.sam.exchange.transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import dev.sam.exchange.persistence.RequestLog;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
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
  private static final int MAX_PENDING_LOG_REQUESTS = 8;
  private CommandRequest pendingRequest;
  private long logDeadlineNanos;
  private CommandResponse pendingResponse;
  private int pendingResponseLength;
  private long replyDeadlineNanos;

  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final SbeResponseCodec responseCodec = new SbeResponseCodec();
  private final List<CommandRequest> receivedRequests = new ArrayList<>();
  private final FragmentHandler handler = (buffer, offset, length, header) -> {
    CommandRequest request = requestCodec.decode(buffer, offset, length);
    receivedRequests.add(request);
  };
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final FragmentAssembler assembler = new FragmentAssembler(handler);
  private final Deque<PendingLoggedRequest> pendingLoggedRequests = new ArrayDeque<>();

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
    if (pendingResponse != null) {
      return offerPendingReply();
    }

    int work = fillLogWindow();
    work += processRecordedHead();
    work += offerPendingReply();
    return work;
  }

  private void prepareReply(CommandRequest request) {
    pendingResponse = processor.process(request);
    pendingResponseLength = responseCodec.encode(pendingResponse, buffer, 0);
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

  private int processRecordedHead() {
    if (pendingResponse != null) {
      return 0;
    }
    PendingLoggedRequest head = pendingLoggedRequests.peek();
    if (head == null) {
      return 0;
    }
    boolean isRecorded = requestLog.isRecorded(head.endPosition());
    if (!isRecorded) {
      boolean isExpired = (clock.nanoTime() - head.deadlineNanos()) >= 0;
      if (isExpired) {
        throw new IllegalStateException("Timed out waiting for log record at position " + head.endPosition());
      }
      return 0;
    }
    pendingLoggedRequests.removeFirst();
    prepareReply(head.request());
    return 1;
  }

  private int offerPendingRequest() {
    if (pendingRequest == null || pendingLoggedRequests.size() >= MAX_PENDING_LOG_REQUESTS) {
      return 0;
    }
    UUID requestId = pendingRequest.requestId();
    if (processor.hasProcessed(requestId)
        || pendingLoggedRequests.stream().anyMatch(entry -> entry.request().requestId().equals(requestId))) {
      return 0;
    }
    long position = requestLog.offer(pendingRequest);
    if (position < 0) {
      checkLogDeadline("offering pending request; last offer result: " + position);
      return 0;
    }
    pendingLoggedRequests.addLast(new PendingLoggedRequest(pendingRequest, position, clock.nanoTime() + TIMEOUT_NS));
    pendingRequest = null;
    return 1;
  }

  private int processCachedRequest() {
    if (pendingRequest == null || pendingResponse != null || !pendingLoggedRequests.isEmpty()
        || !processor.hasProcessed(pendingRequest.requestId())) {
      return 0;
    }

    prepareReply(pendingRequest);
    pendingRequest = null;
    return 1;
  }

  private int fillLogWindow() {
    if (this.pendingResponse != null) {
      return 0;
    }
    int work = 0;
    while (pendingLoggedRequests.size() < MAX_PENDING_LOG_REQUESTS) {
      if (pendingRequest == null) {
        int result = requests.poll(assembler, 1);
        work = work + result;
        if (receivedRequests.size() == 0) {
          break;
        } else {
          pendingRequest = receivedRequests.removeFirst();
          logDeadlineNanos = clock.nanoTime() + TIMEOUT_NS;
        }
      }
      int processed = processCachedRequest();
      work = work + processed;
      if (processed == 1) {
        break;
      }
      int offered = offerPendingRequest();
      work = work + offered;
      if (offered == 0) {
        break;
      }
    }
    return work;
  }
}
