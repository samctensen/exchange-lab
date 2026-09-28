package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.gateway.proto.CancelOrder;
import dev.sam.exchange.gateway.proto.ExchangeServiceGrpc;
import dev.sam.exchange.gateway.proto.SubmitRequest;
import dev.sam.exchange.gateway.proto.SubmitResponse;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.protocol.SbeResponseCodec;
import dev.sam.exchange.transport.AeronRequestClient;
import dev.sam.exchange.transport.ClientConfig;
import dev.sam.exchange.transport.CommandRequest;
import dev.sam.exchange.transport.CommandResponse;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

class GrpcGatewayPipelineTest {
  @Test
  @Timeout(20)
  void concurrentRpcCallsRespectTheWindowAndReceiveTheirOwnAeronReplies(@TempDir Path tempDir) throws Exception {
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    try (
        MediaDriver driver = MediaDriver
            .launchEmbedded(new MediaDriver.Context().aeronDirectoryName(tempDir.resolve("aeron").toString())
                .dirDeleteOnShutdown(true).errorHandler(errors::add));
        Aeron aeron = Aeron
            .connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()).errorHandler(errors::add));
        Publication requests = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        Subscription peerRequests = aeron.addSubscription("aeron:ipc", 1);
        Publication peerReplies = aeron.addPublication("aeron:ipc", 2);
        EngineGateway gateway = new EngineGateway(
            new AeronRequestClient(requests, replies, new ClientConfig(Duration.ofSeconds(5), 1)), 128, 8)) {
      awaitConnected(requests);
      awaitConnected(peerReplies);
      gateway.start();
      Server server = ServerBuilder.forPort(0).addService(new GrpcExchangeService(gateway, new GrpcCommandMapper()))
          .build().start();
      ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
      try {
        var stub = ExchangeServiceGrpc.newFutureStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
        List<Future<SubmitResponse>> results = new ArrayList<>();
        for (long id = 1; id <= 16; id++) {
          SubmitRequest request = SubmitRequest.newBuilder().setRequestId(new UUID(0L, id).toString())
              .setCancel(CancelOrder.newBuilder().setOrderId(1000 + id)).build();
          results.add(stub.submit(request));
        }

        SbeRequestCodec requestCodec = new SbeRequestCodec();
        List<CommandRequest> received = new ArrayList<>();
        FragmentHandler handler = (buffer, offset, length, header) -> received
            .add(requestCodec.decode(buffer, offset, length));
        awaitRequests(peerRequests, handler, received, 8);

        // Withhold every reply. A serial gateway cannot reach eight; an unbounded one sends more.
        long observationDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        do {
          int fragments = peerRequests.poll(handler, 16);
          assertEquals(8, received.size(), "No ninth request may be sent while all eight slots are occupied");
          idle.idle(fragments);
        } while (System.nanoTime() - observationDeadline < 0);
        assertTrue(results.stream().noneMatch(Future::isDone), "Every caller must still be waiting for its reply");

        // gRPC can deliver requests in any order. Reverse the actual Aeron arrival order.
        replyInReverse(peerReplies, received.subList(0, 8));
        awaitRequests(peerRequests, handler, received, 16);
        replyInReverse(peerReplies, received.subList(8, 16));
        assertEquals(16, received.stream().map(CommandRequest::requestId).distinct().count());

        for (int index = 0; index < results.size(); index++) {
          long id = index + 1L;
          SubmitResponse response = results.get(index).get(3, TimeUnit.SECONDS);
          assertEquals(new UUID(0L, id).toString(), response.getRequestId());
          assertEquals(SubmitResponse.ResultCase.CANCEL, response.getResultCase());
          assertEquals(1000 + id, response.getCancel().getOrderId());
          assertFalse(response.getCancel().getCancelled());
        }
        assertTrue(errors.isEmpty(), errors::toString);
      } finally {
        channel.shutdownNow();
        server.shutdownNow();
        // If an assertion fails, reject any unsent work while the gateway drains during close.
        requests.close();
        assertTrue(channel.awaitTermination(3, TimeUnit.SECONDS), "gRPC client did not stop");
        assertTrue(server.awaitTermination(3, TimeUnit.SECONDS), "gRPC server did not stop");
      }
    }
  }

  private static void awaitConnected(Publication publication) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!publication.isConnected()) {
      assertTrue(System.nanoTime() - deadline < 0, "Aeron publication did not connect");
      idle.idle();
    }
  }

  private static void awaitRequests(Subscription subscription, FragmentHandler handler, List<CommandRequest> received,
      int count) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (received.size() < count) {
      int fragments = subscription.poll(handler, 16);
      assertTrue(System.nanoTime() - deadline < 0,
          "Timed out waiting for " + count + " requests; got " + received.size());
      idle.idle(fragments);
    }
    assertEquals(count, received.size());
  }

  private static void replyInReverse(Publication publication, List<CommandRequest> requests) {
    SbeResponseCodec codec = new SbeResponseCodec();
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    for (CommandRequest request : requests.reversed()) {
      int length = codec.encode(
          new CommandResponse(request.requestId(), new CancelResult(request.command().orderId(), false)), buffer, 0);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (publication.offer(buffer, 0, length) < 0) {
        assertTrue(System.nanoTime() - deadline < 0, "Timed out publishing test response");
        idle.idle();
      }
    }
  }
}
