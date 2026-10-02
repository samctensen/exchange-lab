package dev.sam.exchange.gateway;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import io.aeron.Publication;

public class EngineGateway implements AutoCloseable {
  private enum State {
    NEW, RUNNING, CLOSING, CLOSED
  }

  private final Thread worker;
  private final AeronRequestClient client;
  private final ArrayBlockingQueue<PendingRequest> requests;
  private volatile State state = State.NEW;
  private final Object lifecycleLock = new Object();
  private final Map<UUID, InFlightRequest> inFlight = new LinkedHashMap<>();
  private final int maxInFlight;
  private final SbeRequestCodec requestCodec = new SbeRequestCodec();
  private final GatewayDiagnostics diagnostics;

  public EngineGateway(AeronRequestClient client, int capacity, int maxInFlight) {
    this(client, capacity, maxInFlight, null);
  }

  EngineGateway(AeronRequestClient client, int capacity, int maxInFlight, GatewayDiagnostics diagnostics) {
    if (maxInFlight < 1) {
      throw new IllegalArgumentException("maxInFlight must be at least 1");
    }
    this.client = client;
    this.requests = new ArrayBlockingQueue<>(capacity);
    this.maxInFlight = maxInFlight;
    this.diagnostics = diagnostics;
    this.worker = new Thread(this::runWorker, "engine-gateway");
  }

  public EngineGateway(AeronRequestClient client, int capacity) {
    this(client, capacity, 1);
  }

  public CompletableFuture<CommandResult> submit(CommandRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("request must not be null");
    }
    CompletableFuture<CommandResult> result = new CompletableFuture<>();
    PendingRequest pendingRequest = new PendingRequest(request, result, diagnostics == null ? 0 : System.nanoTime());
    RejectedExecutionException rejection = null;
    // Admission and shutdown share a lock: a request is either accepted before close or rejected.
    synchronized (lifecycleLock) {
      if (state != State.RUNNING) {
        rejection = new RejectedExecutionException("Gateway is not running");
      } else if (!requests.offer(pendingRequest)) {
        rejection = new GatewayOverloadedException("request queue is full");
        if (diagnostics != null)
          diagnostics.queueFull();
      } else if (diagnostics != null) {
        // The worker may dequeue between offer and size; this is an observed high-water mark.
        diagnostics.accepted(requests.size());
      }
    }
    if (rejection != null) {
      result.completeExceptionally(rejection);
    }
    return result;
  }

  private void runWorker() {
    try {
      ClientConfig config = client.config();
      long timeoutNanos = config.timeout().toNanos();
      IdleStrategy idle = new SleepingIdleStrategy();

      while (!Thread.currentThread().isInterrupted()
          && (state == State.RUNNING || !requests.isEmpty() || !inFlight.isEmpty())) {

        int work = doWork(System.nanoTime(), timeoutNanos, config.maxAttempts());

        idle.idle(work);
      }
    } finally {
      synchronized (lifecycleLock) {
        state = State.CLOSED;
      }
      var iterator = inFlight.values().iterator();
      while (iterator.hasNext()) {
        InFlightRequest active = iterator.next();
        // Remove before completing: completion callbacks can run on this worker.
        iterator.remove();
        String message = "Gateway worker stopped; request " + active.pending.request().requestId()
            + (active.attempts > 0 ? "; outcome unknown" : "");
        active.pending.result().completeExceptionally(new IllegalStateException(message));
      }
      PendingRequest pending;
      while ((pending = requests.poll()) != null) {
        pending.result().completeExceptionally(new IllegalStateException("Gateway worker stopped"));
      }
    }
  }

  public void start() {
    synchronized (lifecycleLock) {
      if (state != State.NEW) {
        throw new IllegalStateException("Gateway is not in NEW state");
      }
      state = State.RUNNING;
      worker.start();
    }
  }

  // Stop admission, drain accepted requests, and leave the borrowed Aeron client/resources open.
  @Override
  public void close() {
    // CompletableFuture callbacks can run on this worker; joining it here would deadlock.
    if (Thread.currentThread() == worker) {
      throw new IllegalStateException("Gateway cannot be closed from its worker thread");
    }
    synchronized (lifecycleLock) {
      if (state == State.NEW) {
        state = State.CLOSED;
      } else if (state == State.RUNNING) {
        state = State.CLOSING;
      }
    }
    // Join outside the lock: the worker needs it to mark itself CLOSED in its finally block.
    // Even a repeated close waits for any cleanup still in progress.
    try {
      worker.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for gateway worker to stop", e);
    }
  }

  private int doWork(long nowNanos, long timeoutNanos, int maxAttempts) {
    int work = client.pollResponses(this::handleResponse, 10);
    work += expireRequests(nowNanos, timeoutNanos, maxAttempts);
    work += admitRequests(nowNanos, timeoutNanos);
    work += offerRequests(nowNanos, timeoutNanos);
    return work;
  }

  private int admitRequests(long nowNanos, long timeoutNanos) {
    int workCounter = 0;
    while (inFlight.size() < maxInFlight) {
      PendingRequest pending = requests.peek();
      if (pending == null) {
        break;
      }
      UUID requestId = pending.request().requestId();
      if (inFlight.containsKey(requestId)) {
        break;
      } else {
        requests.poll();
        long activatedNanos = diagnostics == null ? 0 : System.nanoTime();
        InFlightRequest active = new InFlightRequest(pending, requestCodec, nowNanos + timeoutNanos);
        inFlight.put(requestId, active);
        if (diagnostics != null)
          diagnostics.activated(activatedNanos - pending.enqueuedNanos(), inFlight.size());
      }
      workCounter++;
    }
    return workCounter;
  }

  private void handleResponse(CommandResponse response) {
    InFlightRequest active = inFlight.get(response.requestId());
    if (active == null || active.attempts == 0) {
      return;
    }
    inFlight.remove(response.requestId());
    active.pending.result().complete(response.result());
  }

  private int offerRequests(long nowNanos, long timeoutNanos) {
    var iterator = inFlight.entrySet().iterator();
    int workCounter = 0;
    while (iterator.hasNext()) {
      InFlightRequest active = iterator.next().getValue();
      if (active.phase == InFlightRequest.Phase.OFFERING) {
        long result = client.trySend(active.buffer, 0, active.messageLength);
        if (result >= 0) {
          if (diagnostics != null)
            diagnostics.offered(active.attempts > 0);
          active.attempts = active.attempts + 1;
          active.phase = InFlightRequest.Phase.AWAITING_REPLY;
          active.deadlineNanos = nowNanos + timeoutNanos;
          workCounter++;
        } else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
          iterator.remove();
          String message = "Offer failed for " + active.pending.request().requestId() + "; result=" + result
              + (active.attempts > 0 ? "; outcome unknown" : "");
          active.pending.result().completeExceptionally(new IllegalStateException(message));
          workCounter++;
        } else {
          break;
        }
      }
    }
    return workCounter;
  }

  private int expireRequests(long nowNanos, long timeoutNanos, int maxAttempts) {
    var iterator = inFlight.entrySet().iterator();
    int workCounter = 0;
    while (iterator.hasNext()) {
      InFlightRequest active = iterator.next().getValue();
      if (nowNanos - active.deadlineNanos < 0) {
        continue;
      } else if (active.phase == InFlightRequest.Phase.OFFERING) {
        if (diagnostics != null)
          diagnostics.timedOut(false);
        iterator.remove();
        String message = "Offer failed for " + active.pending.request().requestId() + "; sending timed out"
            + (active.attempts > 0 ? "; outcome unknown" : "");
        active.pending.result().completeExceptionally(new IllegalStateException(message));
        workCounter++;
      } else if (active.phase == InFlightRequest.Phase.AWAITING_REPLY && active.attempts < maxAttempts) {
        if (diagnostics != null)
          diagnostics.timedOut(true);
        active.phase = InFlightRequest.Phase.OFFERING;
        active.deadlineNanos = nowNanos + timeoutNanos;
        workCounter++;
      } else if (active.phase == InFlightRequest.Phase.AWAITING_REPLY && active.attempts >= maxAttempts) {
        if (diagnostics != null)
          diagnostics.timedOut(true);
        iterator.remove();
        String message = "No reply after " + active.attempts + " attempts for request "
            + active.pending.request().requestId() + (active.attempts > 0 ? "; outcome unknown" : "");
        active.pending.result().completeExceptionally(new IllegalStateException(message));
        workCounter++;
      }
    }
    return workCounter;
  }
}
