package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;

@Timeout(10)
class EngineGatewayDiagnosticsTest {
  @Test
  void lifecycleRejectionsDoNotInflateAcceptedOrQueueFull() throws Exception {
    var diagnostics = new GatewayDiagnostics();
    var client = new ControlledClient();
    try (var gateway = new EngineGateway(client, 1, 1, diagnostics)) {
      assertRejected(gateway, 90);
      gateway.start();
      await(client.pollEntered);
      var accepted = gateway.submit(request(1));
      assertRejected(gateway, 2);
      var blocked = diagnostics.snapshot();
      assertEquals(1, blocked.accepted());
      assertEquals(1, blocked.queueFull());
      assertEquals(1, blocked.notRunning());
      client.automaticReplies = true;
      client.pollRelease.countDown();
      assertEquals(new CancelResult(1, false), accepted.get(3, TimeUnit.SECONDS));
      gateway.close();
      assertRejected(gateway, 91);
      var closed = diagnostics.snapshot();
      assertEquals(1, closed.accepted());
      assertEquals(1, closed.queueFull());
      assertEquals(2, closed.notRunning());
      assertTrue(closed.summarize().contains("Gateway not running: 2"));
    } finally {
      client.pollRelease.countDown();
    }
  }

  @Test
  void workerFailureRejectsNewAdmissionWithoutCallingItOverload() {
    var diagnostics = new GatewayDiagnostics();
    var client = new ControlledClient();
    try (var gateway = new EngineGateway(client, 1, 1, diagnostics)) {
      gateway.start();
      await(client.pollEntered);
      var accepted = gateway.submit(request(1));
      client.pollFailure = new IllegalStateException("transport stopped");
      client.pollRelease.countDown();
      assertThrows(CompletionException.class, accepted::join);
      assertRejected(gateway, 2);
      assertEquals(1, diagnostics.snapshot().accepted());
      assertEquals(0, diagnostics.snapshot().queueFull());
      assertEquals(1, diagnostics.snapshot().notRunning());
    } finally {
      client.pollRelease.countDown();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void definitiveResultsAreCountedBeforeCompletionCallbacks(boolean rejected) throws Exception {
    var diagnostics = new GatewayDiagnostics();
    var client = new ControlledClient();
    client.pollRelease.countDown();
    try (var gateway = new EngineGateway(client, 1, 1, diagnostics)) {
      gateway.start();
      var future = gateway.submit(request(1));
      var observed = future.thenApply(result -> diagnostics.snapshot());
      assertNotNull(client.offered.poll(3, TimeUnit.SECONDS));
      CommandResult result = rejected
          ? new RejectResult(1, RejectReason.DUPLICATE_ORDER_ID)
          : new CancelResult(1, false);
      client.replies.add(new CommandResponse(request(1).requestId(), result));
      assertEquals(result, future.get(3, TimeUnit.SECONDS));
      var snapshot = observed.get(3, TimeUnit.SECONDS);
      assertEquals(1, snapshot.completed());
      assertEquals(rejected ? 1 : 0, snapshot.businessRejected());
      assertTrue(snapshot.summarize().contains("Gateway completed: 1"));
      assertTrue(snapshot.summarize().contains("Gateway business rejected: " + (rejected ? 1 : 0)));
    }
  }

  @Test
  void callerCancellationDoesNotEraseAnObservedBusinessResult() throws Exception {
    var diagnostics = new GatewayDiagnostics();
    var client = new ControlledClient();
    client.pollRelease.countDown();
    try (var gateway = new EngineGateway(client, 2, 2, diagnostics)) {
      gateway.start();
      var cancelled = gateway.submit(request(1));
      assertNotNull(client.offered.poll(3, TimeUnit.SECONDS));
      assertTrue(cancelled.cancel(false));
      var next = gateway.submit(request(2));
      assertNotNull(client.offered.poll(3, TimeUnit.SECONDS));
      client.replies
          .add(new CommandResponse(request(1).requestId(), new RejectResult(1, RejectReason.REQUEST_ID_CONFLICT)));
      client.replies.add(new CommandResponse(request(2).requestId(), new CancelResult(2, false)));
      next.get(3, TimeUnit.SECONDS);
      assertTrue(cancelled.isCancelled());
      assertEquals(2, diagnostics.snapshot().completed());
      assertEquals(1, diagnostics.snapshot().businessRejected());
    }
  }

  private static void assertRejected(EngineGateway gateway, long id) {
    var failure = assertThrows(CompletionException.class, () -> gateway.submit(request(id)).join());
    assertInstanceOf(RejectedExecutionException.class, failure.getCause());
  }

  private static CommandRequest request(long id) {
    return new CommandRequest(new UUID(0, id), new CancelOrder(id));
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(3, TimeUnit.SECONDS), "Worker did not reach the expected boundary");
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new AssertionError(error);
    }
  }

  private static class ControlledClient extends AeronRequestClient {
    final CountDownLatch pollEntered = new CountDownLatch(1);
    final CountDownLatch pollRelease = new CountDownLatch(1);
    final LinkedBlockingQueue<CommandRequest> offered = new LinkedBlockingQueue<>();
    final ConcurrentLinkedQueue<CommandResponse> replies = new ConcurrentLinkedQueue<>();
    volatile RuntimeException pollFailure;
    volatile boolean automaticReplies;
    private final SbeRequestCodec codec = new SbeRequestCodec();

    ControlledClient() {
      super(null, null, new ClientConfig(Duration.ofSeconds(3), 1));
    }

    @Override
    public long trySend(DirectBuffer buffer, int offset, int length) {
      var request = codec.decode(buffer, offset, length);
      offered.add(request);
      if (automaticReplies)
        replies.add(new CommandResponse(request.requestId(),
            new CancelResult(((CancelOrder) request.command()).orderId(), false)));
      return 128;
    }

    @Override
    public int pollResponses(Consumer<CommandResponse> onResponse, int fragmentLimit) {
      pollEntered.countDown();
      await(pollRelease);
      if (pollFailure != null)
        throw pollFailure;
      int work = 0;
      CommandResponse response;
      while (work < fragmentLimit && (response = replies.poll()) != null) {
        onResponse.accept(response);
        work++;
      }
      return work;
    }
  }
}
