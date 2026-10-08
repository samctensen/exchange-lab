package dev.sam.exchange.transport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

import dev.sam.exchange.persistence.RequestLog;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import dev.sam.exchange.transport.ReplyDeliveryStats.DropReason;
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
  private final ReplyDeliveryConfig replyConfig;
  private final long replyTimeoutNanos;
  private final ReplyDeliveryStats replyStats;

  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

  static final int DEFAULT_LOG_WINDOW = 8;
  private RoutedRequest pendingRequest;
  private long logDeadlineNanos;
  private long admittedNanos;

  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final SbeResponseCodec responseCodec = new SbeResponseCodec();
  private final List<RoutedRequest> receivedRequests = new ArrayList<>();
  private final FragmentHandler handler = (buffer, offset, length, header) -> {
    CommandRequest request = requestCodec.decode(buffer, offset, length);
    Image image = (Image) header.context();
    // A new image can arrive after the duty cycle's scan and before this poll.
    registerResponsePublication(image);
    receivedRequests.add(new RoutedRequest(request, image.correlationId()));
  };
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
  private final FragmentAssembler assembler = new FragmentAssembler(handler);
  private final Deque<PendingLoggedRequest> pendingLoggedRequests = new ArrayDeque<>();
  private final Map<Long, Image> registeredImages = new HashMap<>();
  private final Map<Long, PendingReplyQueue> replyQueues = new HashMap<>();

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
    this(requests, responsePublications, processor, requestLog, logResults, clock, stageTimings, logWindow,
        ReplyDeliveryConfig.defaults());
  }

  AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, NanoClock clock,
      EngineStageTimings stageTimings, int logWindow, ReplyDeliveryConfig replyConfig) {
    this(requests, responsePublications, processor, requestLog, logResults, clock, stageTimings, logWindow, replyConfig,
        null);
  }

  AeronEngineAgent(Subscription requests, ResponsePublicationRegistry responsePublications,
      RequestStateMachine processor, RequestLog requestLog, boolean logResults, NanoClock clock,
      EngineStageTimings stageTimings, int logWindow, ReplyDeliveryConfig replyConfig, ReplyDeliveryStats replyStats) {
    this.logWindow = validateLogWindow(logWindow);
    this.replyConfig = Objects.requireNonNull(replyConfig, "replyConfig");
    this.replyTimeoutNanos = replyConfig.timeout().toNanos();
    this.replyStats = replyStats;
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
    int work = syncResponsePublications();
    // Give existing replies a turn and free queue capacity before preparing another result.
    work += offerQueuedReplies();
    work += fillLogWindow();
    work += processRecordedHead();
    return work;
  }

  private void prepareReply(RoutedRequest routedRequest, PendingLoggedRequest timedRequest, long recordedNanos) {
    // A recorded command must execute even if its connection has departed. Retries use the cached result.
    CommandResponse response = processor.process(routedRequest.request());
    long route = routedRequest.responseCorrelationId();
    PendingReplyQueue queue = replyQueues.get(route);
    if (!enqueueReply(route, response, timedRequest, recordedNanos)) {
      // Missing routes and full queues affect delivery only; never roll back or rerun the command.
      recordDropped(queue == null ? DropReason.UNAVAILABLE : DropReason.CAPACITY, queue == null ? 1 : queue.size() + 1);
      replyQueues.remove(route);
      responsePublications.remove(route);
    }
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

  private int processRecordedHead() {
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
    // This includes FIFO delay and when the agent observes progress, not just Archive disk work.
    long recordedNanos = stageTimings == null ? 0 : clock.nanoTime();
    pendingLoggedRequests.removeFirst();
    prepareReply(head.routedRequest(), stageTimings == null ? null : head, recordedNanos);
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
    if (pendingRequest == null || !pendingLoggedRequests.isEmpty()
        || !processor.hasProcessed(pendingRequest.request().requestId())) {
      return 0;
    }

    prepareReply(pendingRequest, null, 0);
    pendingRequest = null;
    return 1;
  }

  private int fillLogWindow() {
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
        PendingReplyQueue removed = replyQueues.remove(entry.getKey());
        if (removed != null) {
          recordDropped(DropReason.DISCONNECTED, removed.size());
        }
        responsePublications.remove(entry.getKey());
        assembler.freeSessionBuffer(entry.getValue().sessionId());
        iterator.remove();
        work++;
      }
    }

    // Get one snapshot of the subscription's current images.
    for (Image image : requests.images()) {
      work += registerResponsePublication(image);
    }

    return work;
  }

  private int registerResponsePublication(Image image) {
    long correlationId = image.correlationId();
    if (image.isClosed() || registeredImages.containsKey(correlationId)) {
      return 0;
    }
    responsePublications.register(correlationId);
    registeredImages.put(correlationId, image);
    replyQueues.put(correlationId, new PendingReplyQueue(replyConfig.maxReplies(), replyConfig.maxBytes()));
    return 1;
  }

  private boolean enqueueReply(long correlationId, CommandResponse response, PendingLoggedRequest timedRequest,
      long recordedNanos) {
    var queue = replyQueues.get(correlationId);
    if (queue == null) {
      return false;
    }
    int encodedLength = responseCodec.encodedLength(response);
    long preparedNanos = clock.nanoTime();
    PendingReply reply = new PendingReply(response, preparedNanos + replyTimeoutNanos, timedRequest, recordedNanos,
        preparedNanos);
    return queue.offer(reply, encodedLength);
  }

  private int offerQueuedReplies() {
    int work = 0;
    var iterator = replyQueues.entrySet().iterator();

    while (iterator.hasNext()) {
      var entry = iterator.next();
      PendingReplyQueue queue = entry.getValue();
      PendingReply reply = queue.peek();
      if (reply == null) {
        continue;
      }

      // Registration and connection waits use the original deadline too.
      if (clock.nanoTime() - reply.deadlineNanos() >= 0) {
        recordDropped(DropReason.EXPIRED, queue.size());
        responsePublications.remove(entry.getKey());
        iterator.remove();
        // Keep registeredImages: this connection stays disabled until its request image closes.
        work++;
        continue;
      }

      var publication = responsePublications.find(entry.getKey());
      if (publication.isEmpty()) {
        continue;
      }

      // Each queue holds response objects; another route may have overwritten the shared buffer.
      int length = responseCodec.encode(reply.response(), buffer, 0);
      long offerResult = publication.get().offer(buffer, 0, length);
      if (offerResult >= 0) {
        queue.poll();
        if (replyStats != null) {
          replyStats.sent();
        }
        PendingLoggedRequest timedRequest = reply.timedRequest();
        if (stageTimings != null && timedRequest != null) {
          stageTimings.record(timedRequest.admittedNanos(), timedRequest.offeredNanos(), reply.recordedNanos(),
              reply.preparedNanos(), clock.nanoTime());
        }
        if (logResults) {
          System.out.println("Result: " + reply.response().result());
        }
        work++;
      } else if (offerResult == Publication.CLOSED || offerResult == Publication.MAX_POSITION_EXCEEDED) {
        recordDropped(DropReason.UNAVAILABLE, queue.size());
        responsePublications.remove(entry.getKey());
        iterator.remove();
        work++;
      }
      // Try only one head per route. A retryable failure leaves it queued while other routes get a turn.
    }

    return work;
  }

  private void recordDropped(DropReason reason, int count) {
    if (replyStats != null) {
      replyStats.dropped(reason, count);
    }
  }
}
