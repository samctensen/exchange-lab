package dev.sam.exchange.transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

public class AeronEngineAgent implements Agent {
  private final Subscription requests;
  private final ResponsePublicationRegistry responsePublications;
  private final RequestStateMachine processor;
  private final RequestLog requestLog;
  private final NanoClock clock;
  private final boolean logResults;
  private final EngineStageTimings stageTimings;
  private final int logWindow;

  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
  static final int DEFAULT_LOG_WINDOW = 8;
  private RoutedRequest pendingRequest;
  private long pendingResponseCorrelationId;
  private long logDeadlineNanos;
  private CommandResponse pendingResponse;
  private int pendingResponseLength;
  private long replyDeadlineNanos;
  private long admittedNanos;
  private PendingLoggedRequest timedReply;
  private long recordedNanos;
  private long preparedNanos;

  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final SbeResponseCodec responseCodec = new SbeResponseCodec();
  private final List<RoutedRequest> receivedRequests = new ArrayList<>();
  private final FragmentHandler handler = (buffer, offset, length, header) -> {
    CommandRequest request = requestCodec.decode(buffer, offset, length);
    Image image = (Image) header.context();
    receivedRequests.add(new RoutedRequest(request, image.correlationId()));
  };
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final FragmentAssembler assembler = new FragmentAssembler(handler);
  private final Deque<PendingLoggedRequest> pendingLoggedRequests = new ArrayDeque<>();
  private final Map<Long, Image> registeredImages = new HashMap<>();

  public AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog) {
    this(requests, responsePublications, processor, requestLog, true);
  }

  public AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults) {
    this(requests, responsePublications, processor, requestLog, logResults, SystemNanoClock.INSTANCE);
  }

  public AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, int logWindow) {
    this(requests, responsePublications, processor, requestLog, logResults, SystemNanoClock.INSTANCE, null, logWindow);
  }

  AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, NanoClock clock) {
    this(requests, responsePublications, processor, requestLog, logResults, clock, null);
  }

  AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, NanoClock clock,
      EngineStageTimings stageTimings) {
    this(requests, responsePublications, processor, requestLog, logResults, clock, stageTimings, DEFAULT_LOG_WINDOW);
  }

  AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, NanoClock clock,
      EngineStageTimings stageTimings, int logWindow) {
    this.logWindow = validateLogWindow(logWindow);
    this.requests = requests;
    this.responsePublications = responsePublications;
    this.processor = processor;
    this.requestLog = requestLog;
    this.logResults = logResults;
    this.clock = clock;
    this.stageTimings = stageTimings;
  }

  static int validateLogWindow(int logWindow) {
    if (logWindow <= 0) {
      throw new IllegalArgumentException("Log window must be positive: " + logWindow);
    }
    return logWindow;
  }

  @Override
  public int doWork() {
    // Discover connections even before their first request, and while an earlier reply is pending.
    int work = syncResponsePublications();
    if (pendingResponse != null) {
      return work + offerPendingReply();
    }

    work += fillLogWindow();
    work += processRecordedHead();
    work += offerPendingReply();
    return work;
  }

  private void prepareReply(RoutedRequest routedRequest) {
    pendingResponse = processor.process(routedRequest.request());
    pendingResponseCorrelationId = routedRequest.responseCorrelationId();

    pendingResponseLength = responseCodec.encode(pendingResponse, buffer, 0);
    preparedNanos = clock.nanoTime();
    replyDeadlineNanos = preparedNanos + TIMEOUT_NS;
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

    var publication = responsePublications.find(pendingResponseCorrelationId);
    // Registration and connection waits share the deadline set when this reply was prepared.
    long offerResult = publication.isPresent()
        ? publication.get().offer(buffer, 0, pendingResponseLength)
        : Publication.NOT_CONNECTED;

    if (offerResult >= 0) {
      if (timedReply != null) {
        stageTimings.record(timedReply.admittedNanos(), timedReply.offeredNanos(), recordedNanos, preparedNanos,
            clock.nanoTime());
        timedReply = null;
      }
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
    if (stageTimings != null) {
      // This includes FIFO delay and when the agent observes progress, not just Archive disk work.
      recordedNanos = clock.nanoTime();
      timedReply = head;
    }
    pendingLoggedRequests.removeFirst();
    prepareReply(head.routedRequest());
    return 1;
  }

  private int offerPendingRequest() {
    if (pendingRequest == null || pendingLoggedRequests.size() >= logWindow) {
      return 0;
    }
    UUID requestId = pendingRequest.request().requestId();
    if (processor.hasProcessed(requestId) || pendingLoggedRequests.stream()
        .anyMatch(entry -> entry.routedRequest().request().requestId().equals(requestId))) {
      return 0;
    }
    long position = requestLog.offer(pendingRequest.request());
    if (position < 0) {
      checkLogDeadline("offering pending request; last offer result: " + position);
      return 0;
    }
    long offeredNanos = clock.nanoTime();
    pendingLoggedRequests.addLast(
        new PendingLoggedRequest(pendingRequest, position, offeredNanos + TIMEOUT_NS, admittedNanos, offeredNanos));
    pendingRequest = null;
    return 1;
  }

  private int processCachedRequest() {
    if (pendingRequest == null || pendingResponse != null || !pendingLoggedRequests.isEmpty()
        || !processor.hasProcessed(pendingRequest.request().requestId())) {
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
    while (pendingLoggedRequests.size() < logWindow) {
      if (pendingRequest == null) {
        int result = requests.poll(assembler, 1);
        work = work + result;
        if (receivedRequests.size() == 0) {
          break;
        } else {
          pendingRequest = receivedRequests.removeFirst();
          admittedNanos = clock.nanoTime();
          logDeadlineNanos = admittedNanos + TIMEOUT_NS;
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

  private int syncResponsePublications() {
    int work = 0;

    // Aeron marks an image closed when it becomes unavailable.
    var iterator = registeredImages.entrySet().iterator();

    while (iterator.hasNext()) {
      var entry = iterator.next();

      if (entry.getValue().isClosed()) {
        responsePublications.remove(entry.getKey());
        assembler.freeSessionBuffer(entry.getValue().sessionId());
        iterator.remove();
        work++;
      }
    }

    // Get one snapshot of the subscription's current images.
    for (Image image : requests.images()) {
      if (image.isClosed()) {
        continue;
      }

      long correlationId = image.correlationId();

      if (!registeredImages.containsKey(correlationId)) {
        responsePublications.register(correlationId);
        registeredImages.put(correlationId, image);
        work++;
      }
    }

    return work;
  }
}
