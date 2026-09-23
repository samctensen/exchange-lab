package dev.sam.exchange.gateway;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.CommandRequest;

public class EngineGateway implements AutoCloseable {
  private enum State {
    NEW, RUNNING, CLOSING, CLOSED
  }

  private final Thread worker;
  private final AeronRequestClient client;
  private final ArrayBlockingQueue<PendingRequest> requests;
  private volatile State state = State.NEW;
  private final Object lifecycleLock = new Object();

  public EngineGateway(AeronRequestClient client, int capacity) {
    this.client = client;
    this.requests = new ArrayBlockingQueue<>(capacity);
    this.worker = new Thread(this::runWorker, "engine-gateway");
  }

  public CompletableFuture<CommandResult> submit(CommandRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("request must not be null");
    }
    CompletableFuture<CommandResult> result = new CompletableFuture<>();
    PendingRequest pendingRequest = new PendingRequest(request, result);
    RejectedExecutionException rejection = null;
    // Admission and shutdown share a lock: a request is either accepted before close or rejected.
    synchronized (lifecycleLock) {
      if (state != State.RUNNING) {
        rejection = new RejectedExecutionException("Gateway is not running");
      } else if (!requests.offer(pendingRequest)) {
        rejection = new RejectedExecutionException("request queue is full");
      }
    }
    if (rejection != null) {
      result.completeExceptionally(rejection);
    }
    return result;
  }

  private void process(PendingRequest pending) {
    try {
      CommandResult result = client.send(pending.request());
      pending.result().complete(result);
    } catch (RuntimeException e) {
      pending.result().completeExceptionally(e);
    }
  }

  private void runWorker() {
    try {
      while (state == State.RUNNING || !requests.isEmpty()) {
        PendingRequest pending = requests.poll(100, TimeUnit.MILLISECONDS);
        if (pending != null) {
          process(pending);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      synchronized (lifecycleLock) {
        state = State.CLOSED;
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
}
