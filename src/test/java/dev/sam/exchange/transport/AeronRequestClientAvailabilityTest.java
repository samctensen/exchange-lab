package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.protocol.SbeResponseCodec;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

@Timeout(15)
class AeronRequestClientAvailabilityTest {
  @ParameterizedTest(name = "close publication: {0}")
  @ValueSource(booleans = {true, false})
  void observesConnectionAndClosedResourcesWithoutSending(boolean closePublication, @TempDir Path directory) {
    try (MediaDriver driver = driver(directory);
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication requests = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(),
            1)) {
      var client = new AeronRequestClient(requests, replies);
      assertFalse(client.isTransportAvailable());
      try (Subscription commands = aeron.addSubscription("aeron:ipc", 1)) {
        await(client::isTransportAvailable);
        assertFalse(replies.isConnected(), "A reply publication is created lazily after the first command");
        long position = requests.position();
        for (int i = 0; i < 100; i++)
          assertTrue(client.isTransportAvailable());
        assertEquals(position, requests.position(), "Availability must not publish a probe command");
        assertEquals(0, commands.poll((buffer, offset, length, header) -> fail("Unexpected command"), 10));
        if (closePublication)
          requests.close();
        else
          replies.close();
        assertFalse(client.isTransportAvailable());
      }
    }
  }

  @Test
  void availabilityDoesNotConsumeReplies(@TempDir Path directory) {
    try (MediaDriver driver = driver(directory);
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Publication requests = aeron.addPublication("aeron:ipc", 1);
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2);
        Publication responses = aeron.addPublication("aeron:ipc", 2)) {
      var client = new AeronRequestClient(requests, replies);
      await(() -> requests.isConnected() && responses.isConnected() && replies.isConnected());
      var response = new CommandResponse(new UUID(0, 1), new CancelResult(7, false));
      var buffer = new ExpandableArrayBuffer();
      int length = new SbeResponseCodec().encode(response, buffer, 0);
      await(() -> responses.offer(buffer, 0, length) >= 0);
      for (int i = 0; i < 100; i++)
        assertTrue(client.isTransportAvailable());
      List<CommandResponse> received = new ArrayList<>();
      await(() -> {
        client.pollResponses(received::add, 10);
        return !received.isEmpty();
      });
      assertEquals(List.of(response), received);
      commands.close();
      await(() -> !client.isTransportAvailable());
    }
  }

  private static MediaDriver driver(Path directory) {
    return MediaDriver.launchEmbedded(
        new MediaDriver.Context().aeronDirectoryName(directory.resolve("driver").toString()).dirDeleteOnShutdown(true));
  }

  private static void await(BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    var idle = new SleepingIdleStrategy();
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() - deadline < 0, "Transport state did not converge");
      idle.idle();
    }
  }
}
