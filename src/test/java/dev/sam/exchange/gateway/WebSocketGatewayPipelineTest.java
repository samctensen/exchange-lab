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
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelResult;
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

class WebSocketGatewayPipelineTest {
  @Test
  @Timeout(20)
  void concurrentSocketsRespectTheWindowAndReceiveTheirOwnAeronReplies(@TempDir Path tempDir) throws Exception {
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
      try (WebSocketGateway server = new WebSocketGateway(gateway, 0)) {
        server.start();
        try (var first = new WebSocketTestClient(server.port()); var second = new WebSocketTestClient(server.port())) {
          for (long id = 1; id <= 16; id++) {
            String request = "{\"requestId\":\"" + new UUID(0L, id) + "\",\"cancel\":{\"orderId\":\"" + (1000 + id)
                + "\"}}";
            var client = id % 2 == 1 ? first : second;
            client.socket.sendText(request, true).get(3, TimeUnit.SECONDS);
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
          // The two connections can interleave. Reverse the actual Aeron arrival order.
          replyInReverse(peerReplies, received.subList(0, 8));
          awaitRequests(peerRequests, handler, received, 16);
          replyInReverse(peerReplies, received.subList(8, 16));
          assertEquals(16, received.stream().map(CommandRequest::requestId).distinct().count());

          var seen = new HashSet<UUID>();
          for (int index = 0; index < 16; index++) {
            var client = index % 2 == 0 ? first : second;
            var response = client.response();
            UUID requestId = UUID.fromString(response.get("requestId").getAsString());
            long id = requestId.getLeastSignificantBits();
            assertTrue(id >= 1 && id <= 16);
            assertEquals(index % 2 == 0 ? 1 : 0, id % 2, "Reply reached the wrong socket");
            assertTrue(seen.add(requestId), "Duplicate response");
            assertEquals(Long.toString(1000 + id), response.getAsJsonObject("cancel").get("orderId").getAsString());
            assertFalse(response.getAsJsonObject("cancel").get("cancelled").getAsBoolean());
          }
          assertEquals(16, seen.size());
          assertTrue(errors.isEmpty(), errors::toString);
        }
      } finally {
        // If an assertion fails, reject any unsent work while the gateway drains during close.
        requests.close();
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
