package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.CloseHelper;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

@Timeout(15)
class ResponsePublicationRegistryTest {
  @Test
  void unknownRouteDoesNotCreateAPublication() {
    try (Fixture fixture = new Fixture()) {
      assertTrue(fixture.registry.find(fixture.routeA).isEmpty());
      fixture.registry.remove(fixture.routeA);
      assertFalse(fixture.aeron.hasActiveCommands());
      assertFalse(fixture.clientA.replies().isConnected());
    }
  }

  @Test
  void lookupIsEmptyWhileRegistrationIsPendingThenReturnsThePublication() {
    try (Fixture fixture = new Fixture()) {
      fixture.registry.register(fixture.routeA);

      // The driver is manually invoked, so it cannot complete this registration yet.
      assertTrue(fixture.registry.find(fixture.routeA).isEmpty());

      ConcurrentPublication replies = fixture.awaitPublication(fixture.routeA);
      assertTrue(replies.isConnected());
      assertSame(replies, fixture.registry.find(fixture.routeA).orElseThrow());
    }
  }

  @Test
  void duplicateRegistrationReusesPendingAndReadyPublications() {
    try (Fixture fixture = new Fixture()) {
      fixture.registry.register(fixture.routeA);
      fixture.registry.register(fixture.routeA);
      ConcurrentPublication replies = fixture.awaitPublication(fixture.routeA);

      fixture.registry.register(fixture.routeA);
      assertSame(replies, fixture.registry.find(fixture.routeA).orElseThrow());

      fixture.registry.remove(fixture.routeA);
      fixture.await(() -> !fixture.aeron.hasActiveCommands(), "removing the reused publication");
      fixture.await(() -> !fixture.clientA.replies().isConnected(), "disconnecting the removed route");
      assertTrue(replies.isClosed());
    }
  }

  @Test
  void publicationsDeliverRepliesOnlyToTheirAssociatedClients() {
    try (Fixture fixture = new Fixture()) {
      fixture.registry.register(fixture.routeA);
      fixture.registry.register(fixture.routeB);
      ConcurrentPublication repliesToA = fixture.awaitPublication(fixture.routeA);
      ConcurrentPublication repliesToB = fixture.awaitPublication(fixture.routeB);
      assertNotSame(repliesToA, repliesToB);

      fixture.send(repliesToA, "reply for A");
      fixture.send(repliesToB, "reply for B");
      List<String> receivedByA = new ArrayList<>();
      List<String> receivedByB = new ArrayList<>();
      fixture.await(() -> {
        fixture.clientA.replies()
            .poll((buffer, offset, length, header) -> receivedByA.add(buffer.getStringAscii(offset)), 10);
        fixture.clientB.replies()
            .poll((buffer, offset, length, header) -> receivedByB.add(buffer.getStringAscii(offset)), 10);
        return !receivedByA.isEmpty() && !receivedByB.isEmpty();
      }, "receiving both replies");

      assertEquals(List.of("reply for A"), receivedByA);
      assertEquals(List.of("reply for B"), receivedByB);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void removeReleasesOnlyTheSelectedRoute(boolean pending) {
    try (Fixture fixture = new Fixture()) {
      fixture.registry.register(fixture.routeB);
      ConcurrentPublication repliesToB = fixture.awaitPublication(fixture.routeB);
      fixture.registry.register(fixture.routeA);
      ConcurrentPublication repliesToA = pending ? null : fixture.awaitPublication(fixture.routeA);
      if (pending) {
        assertTrue(fixture.registry.find(fixture.routeA).isEmpty());
      }

      fixture.registry.remove(fixture.routeA);
      fixture.registry.remove(fixture.routeA);
      fixture.await(() -> !fixture.aeron.hasActiveCommands(), "removing the selected publication");
      fixture.await(() -> !fixture.clientA.replies().isConnected(), "disconnecting the selected route");

      assertTrue(fixture.registry.find(fixture.routeA).isEmpty());
      if (repliesToA != null) {
        assertTrue(repliesToA.isClosed());
      }
      assertSame(repliesToB, fixture.registry.find(fixture.routeB).orElseThrow());
      fixture.send(repliesToB, "still connected");
      List<String> received = new ArrayList<>();
      fixture.await(() -> {
        fixture.clientB.replies().poll((buffer, offset, length, header) -> received.add(buffer.getStringAscii(offset)),
            10);
        return !received.isEmpty();
      }, "receiving on the surviving route");
      assertEquals(List.of("still connected"), received);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closeReleasesAllRoutesAndPermanentlyRejectsRegistration(boolean pending) {
    try (Fixture fixture = new Fixture()) {
      fixture.registry.register(fixture.routeA);
      fixture.registry.register(fixture.routeB);
      ConcurrentPublication repliesToA = pending ? null : fixture.awaitPublication(fixture.routeA);
      ConcurrentPublication repliesToB = pending ? null : fixture.awaitPublication(fixture.routeB);
      if (pending) {
        assertTrue(fixture.registry.find(fixture.routeA).isEmpty());
        assertTrue(fixture.registry.find(fixture.routeB).isEmpty());
      }

      fixture.registry.close();
      fixture.registry.close();
      fixture.await(() -> !fixture.aeron.hasActiveCommands(), "closing all publications");
      fixture.await(() -> !fixture.clientA.replies().isConnected() && !fixture.clientB.replies().isConnected(),
          "disconnecting both response routes");

      assertTrue(fixture.registry.find(fixture.routeA).isEmpty());
      assertTrue(fixture.registry.find(fixture.routeB).isEmpty());
      if (!pending) {
        assertTrue(repliesToA.isClosed());
        assertTrue(repliesToB.isClosed());
      }
      assertThrows(IllegalStateException.class, () -> fixture.registry.register(fixture.routeA));
    }
  }

  private static final class Fixture implements AutoCloseable {
    // Invoker mode lets tests hold a registration pending until they explicitly advance the driver.
    final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().threadingMode(ThreadingMode.INVOKER)
        .publicationLingerTimeoutNs(0).dirDeleteOnShutdown(true));
    final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())
        .useConductorAgentInvoker(true).driverAgentInvoker(driver.sharedAgentInvoker()));
    final ResponsePublicationRegistry registry = new ResponsePublicationRegistry(aeron);
    final Subscription requests = aeron.addSubscription("aeron:ipc", 1);
    final Client clientA = createClient();
    final Client clientB = createClient();
    final long routeA;
    final long routeB;

    Fixture() {
      await(() -> requests.imageCount() == 2 && clientA.requests().isConnected() && clientB.requests().isConnected(),
          "connecting both clients");
      routeA = requestImage(clientA).correlationId();
      routeB = requestImage(clientB).correlationId();
    }

    Client createClient() {
      Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
      Publication publication = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(),
          1);
      return new Client(replies, publication);
    }

    Image requestImage(Client client) {
      return requests.imageBySessionId(client.requests().sessionId());
    }

    ConcurrentPublication awaitPublication(long route) {
      await(() -> registry.find(route).map(Publication::isConnected).orElse(false),
          "connecting the response publication");
      return registry.find(route).orElseThrow();
    }

    void send(Publication publication, String message) {
      UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
      int length = buffer.putStringAscii(0, message);
      await(() -> publication.offer(buffer, 0, length) >= 0, "sending " + message);
    }

    void await(BooleanSupplier completed, String operation) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      do {
        driver.sharedAgentInvoker().invoke();
        aeron.conductorAgentInvoker().invoke();
        if (completed.getAsBoolean()) {
          return;
        }
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
          fail("Timed out or interrupted while " + operation);
        }
        idle.idle();
      } while (true);
    }

    @Override
    public void close() {
      CloseHelper.closeAll(registry, aeron, driver);
    }
  }

  private record Client(Subscription replies, Publication requests) {
  }
}
