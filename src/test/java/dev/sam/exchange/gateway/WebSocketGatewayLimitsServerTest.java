package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.aeron.driver.MediaDriver;

@Timeout(30)
@SuppressWarnings("try") // Subprocess cleanup waits are interruptible in these lifecycle tests.
class WebSocketGatewayLimitsServerTest {
  @TempDir
  Path directory;

  @Test
  void configuredConnectionLimitIsEnforcedByTheLauncher() throws Exception {
    try (Fixture fixture = new Fixture("--max-connections=1");
        WebSocketTestClient first = new WebSocketTestClient(fixture.port);
        Socket excess = new Socket("127.0.0.1", fixture.port)) {
      excess.setSoTimeout(3000);
      assertEquals(-1, excess.getInputStream().read(), "Second connection must be rejected before upgrade");
      assertFalse(first.closed.isDone(), "The admitted connection must remain open");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--max-connections", "--max-pending-per-connection", "--max-message-bytes",
      "--max-response-bytes"})
  void invalidIntegerLimitsFailBeforeConnecting(String flag) {
    for (String value : List.of("0", "-1", "2147483648", "bad", "")) {
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
          () -> WebSocketGatewayServer.main(new String[]{flag + "=" + value}));
      assertTrue(error.getMessage().contains(flag));
      assertFalse(error.getMessage().startsWith("Unknown"), "The option must be recognized and validated");
    }
  }

  @Test
  void connectionFlagRequiresAValueAndCannotBeRepeated() {
    assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{"--max-connections"}));
    assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{"--max-connections=1", "--max-connections=2"}));
  }

  @Test
  void pendingLimitRejectsTheSecondRequestWithoutClaimingItExecuted() throws Exception {
    try (Fixture fixture = new Fixture("--max-pending-per-connection=1", "--timeout-ms=5000", "--max-attempts=1");
        WebSocketTestClient client = new WebSocketTestClient(fixture.port)) {
      client.socket.sendText(WebSocketOrderHandlerTest.cancel(1), true).get(3, TimeUnit.SECONDS);
      client.socket.sendText(WebSocketOrderHandlerTest.cancel(2), true).get(3, TimeUnit.SECONDS);
      var response = client.response();
      assertEquals(WebSocketOrderHandlerTest.id(2), response.get("requestId").getAsString());
      assertEquals("RESOURCE_EXHAUSTED", response.getAsJsonObject("error").get("code").getAsString());
      assertFalse(response.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
    }
  }

  @Test
  void configuredInboundByteLimitReachesTheListeningServer() throws Exception {
    try (Fixture fixture = new Fixture("--max-message-bytes=128");
        WebSocketTestClient client = new WebSocketTestClient(fixture.port)) {
      client.socket.sendText("x".repeat(129), true).get(3, TimeUnit.SECONDS);
      assertEquals(1009, client.closed.get(3, TimeUnit.SECONDS));
    }
  }

  @Test
  void configuredResponseBudgetClosesAnOversizedErrorResponse() throws Exception {
    try (Fixture fixture = new Fixture("--max-response-bytes=1");
        WebSocketTestClient client = new WebSocketTestClient(fixture.port)) {
      client.socket.sendText("{}", true).get(3, TimeUnit.SECONDS);
      assertEquals(1009, client.closed.get(3, TimeUnit.SECONDS));
      assertTrue(client.closeReason.get(3, TimeUnit.SECONDS).contains("outcome unknown"));
    }
  }

  private final class Fixture implements AutoCloseable {
    final MediaDriver driver;
    final Process process;
    final Path output;
    final int port;

    Fixture(String... arguments) throws Exception {
      driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
          .aeronDirectoryName(directory.resolve("driver").toString()).dirDeleteOnShutdown(true));
      output = directory.resolve("gateway.log");
      Process started = null;
      try {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
            WebSocketGatewayServer.class.getName(), "--port=0", "--aeron-directory=" + driver.aeronDirectoryName()));
        command.addAll(Arrays.asList(arguments));
        started = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        process = started;
        port = awaitPort();
      } catch (Exception | Error failure) {
        if (started != null) {
          started.destroyForcibly();
          started.waitFor(5, TimeUnit.SECONDS);
        }
        driver.close();
        throw failure;
      }
    }

    private int awaitPort() throws Exception {
      String prefix = "WebSocket gateway listening on ";
      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      do {
        var endpoint = Files.readString(output).lines().filter(line -> line.startsWith(prefix)).findFirst();
        if (endpoint.isPresent())
          return URI.create(endpoint.get().substring(prefix.length())).getPort();
        Thread.sleep(5);
      } while (process.isAlive() && System.nanoTime() - deadline < 0);
      return fail("Gateway did not start: " + Files.readString(output));
    }

    @Override
    public void close() throws Exception {
      try {
        process.destroy();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Gateway did not stop: " + Files.readString(output));
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(5, TimeUnit.SECONDS);
        }
        driver.close();
      }
    }
  }
}
