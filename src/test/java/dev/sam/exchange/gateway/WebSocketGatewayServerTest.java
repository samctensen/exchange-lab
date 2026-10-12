package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.CommandRequest;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

class WebSocketGatewayServerTest {
  @TempDir
  Path temporaryDirectory;

  @Test
  void launcherConnectsToTheSelectedAeronDirectory() throws Exception {
    Path driverDirectory = temporaryDirectory.resolve("selected-driver");
    Path output = temporaryDirectory.resolve("gateway.log");
    try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
        .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true))) {
      Process gateway = startGateway(output, driver.aeronDirectoryName());
      try {
        assertEquals("127.0.0.1", awaitEndpoint(gateway, output).getHost());
      } finally {
        stopGateway(gateway);
      }
    }
  }

  @Test
  void launcherUsesConfiguredRequestTimeout() throws Exception {
    assertTimedOutRequestAttempts(3, "--timeout-ms=20");
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "9223372036854"})
  void acceptsTimeoutBoundaries(String milliseconds) {
    assertDoesNotThrow(() -> WebSocketGatewayServer.Config.parse(new String[]{"--timeout-ms=" + milliseconds}));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "9223372036855", "9223372036854775807", "9223372036854775808", "oops", ""})
  void rejectsInvalidTimeoutBeforeConnectingToAeron(String milliseconds) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.main(new String[]{"--timeout-ms=" + milliseconds}));
    assertTrue(error.getMessage().contains("--timeout-ms"));
  }

  private void assertTimedOutRequestAttempts(int expectedAttempts, String... arguments) throws Exception {
    Path output = temporaryDirectory.resolve("gateway-timeout.log");
    try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
        .aeronDirectoryName(temporaryDirectory.resolve("timeout-driver").toString()).dirDeleteOnShutdown(true));
        Aeron engine = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription requests = engine.addSubscription("aeron:ipc", 1);
        HttpClient http = HttpClient.newHttpClient()) {
      Process gateway = startGateway(output, driver.aeronDirectoryName(), arguments);
      WebSocket socket = null;
      try {
        var responses = new LinkedBlockingQueue<String>();
        socket = http.newWebSocketBuilder().buildAsync(awaitEndpoint(gateway, output), new WebSocket.Listener() {
          private final StringBuilder message = new StringBuilder();

          @Override
          public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            message.append(data);
            if (last) {
              responses.add(message.toString());
              message.setLength(0);
            }
            webSocket.request(1);
            return null;
          }
        }).get(5, TimeUnit.SECONDS);
        long connectionDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!requests.isConnected() && System.nanoTime() - connectionDeadline < 0)
          Thread.sleep(1);
        assertTrue(requests.isConnected(), "Gateway publication did not connect to the request subscription");
        socket.sendText("""
            {"requestId":"11111111-1111-4111-8111-111111111111","cancel":{"orderId":"1"}}
            """, true).get(5, TimeUnit.SECONDS);
        var received = new ArrayList<CommandRequest>();
        var codec = new SbeRequestCodec();
        long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        String response;
        do {
          requests.poll((buffer, offset, length, header) -> received.add(codec.decode(buffer, offset, length)), 10);
          response = responses.poll();
          if (response != null)
            break;
          Thread.sleep(1);
        } while (System.nanoTime() - deadline < 0);
        assertNotNull(response, "Configured timeout did not complete the request within three seconds");
        assertTrue(response.contains("\"outcomeUnknown\":true"), response);
        var expected = new CommandRequest(UUID.fromString("11111111-1111-4111-8111-111111111111"), new CancelOrder(1));
        assertEquals(Collections.nCopies(expectedAttempts, expected), received);
      } finally {
        if (socket != null)
          socket.abort();
        stopGateway(gateway);
      }
    }
  }

  private static Process startGateway(Path output, String directory, String... arguments) throws Exception {
    var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
        WebSocketGatewayServer.class.getName(), "--port=0", "--aeron-directory=" + directory));
    command.addAll(Arrays.asList(arguments));
    return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
  }

  private static URI awaitEndpoint(Process gateway, Path output) throws Exception {
    String prefix = "WebSocket gateway listening on ";
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    String log;
    do {
      log = Files.readString(output);
      var line = log.lines().filter(value -> value.startsWith(prefix)).findFirst();
      if (line.isPresent())
        return URI.create(line.get().substring(prefix.length()));
      Thread.sleep(10);
    } while (gateway.isAlive() && System.nanoTime() - deadline < 0);
    return fail("Gateway did not start: " + Files.readString(output));
  }

  private static void stopGateway(Process gateway) throws Exception {
    gateway.destroy();
    try {
      assertTrue(gateway.waitFor(10, TimeUnit.SECONDS), "Gateway did not stop gracefully");
    } finally {
      if (gateway.isAlive()) {
        gateway.destroyForcibly();
        assertTrue(gateway.waitFor(5, TimeUnit.SECONDS), "Gateway process could not be cleaned up");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--aeron-directory=", "--aeron-directory=   ", "--aeron-directory"})
  void invalidAeronDirectoriesIdentifyTheirFlag(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertTrue(error.getMessage().contains("--aeron-directory"));
  }

  @Test
  void helpPrintsUsageWithoutStartingAeron() throws Exception {
    var output = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try (var capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      assertDoesNotThrow(() -> WebSocketGatewayServer.main(new String[]{"--help"}));
    } finally {
      System.setOut(originalOut);
    }
    String usage = output.toString(StandardCharsets.UTF_8);
    assertTrue(usage.contains("WebSocketGatewayServer"));
    assertTrue(usage.contains("--port"));
    assertTrue(usage.contains("--queue-capacity"));
    assertTrue(usage.contains("--max-in-flight"));
    assertTrue(usage.contains("127.0.0.1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=8080", "--unknown", "--help"})
  void helpMustBeUsedAlone(String otherArgument) {
    for (String[] args : new String[][]{{"--help", otherArgument}, {otherArgument, "--help"}}) {
      var error = assertThrows(IllegalArgumentException.class, () -> WebSocketGatewayServer.main(args));
      assertTrue(error.getMessage().contains("--help must be used alone"));
    }
  }

  @ParameterizedTest
  @CsvSource({"-1,128,8,--port", "65536,128,8,--port", "8080,0,8,--queue-capacity",
      "8080,-1,8,--queue-capacity", "8080,128,0,--max-in-flight", "8080,128,-1,--max-in-flight"})
  void directConfigConstructionRejectsInvalidLimits(int port, int queueCapacity, int maxInFlight, String flag) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> new WebSocketGatewayServer.Config(port, queueCapacity, maxInFlight));
    assertTrue(error.getMessage().contains(flag));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=oops", "--queue-capacity=2147483648", "--max-in-flight="})
  void malformedNumbersIdentifyTheirFlag(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertTrue(error.getMessage().contains(argument.substring(0, argument.indexOf('='))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown=oops", "--unknown", "--unknown="})
  void unknownFlagsAreReportedBeforeTheirValuesAreParsed(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertEquals("Unknown gateway argument: --unknown", error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=-1", "--port=65536", "--queue-capacity=0", "--queue-capacity=-1", "--max-in-flight=0",
      "--max-in-flight=-1", "--max-in-flight=oops", "--port=", "--host=0.0.0.0", "--unknown=1", "--diagnostics=true",
      "--port"})
  void rejectsInvalidFlagsBeforeConnectingToAeron(String flag) {
    assertThrows(IllegalArgumentException.class, () -> WebSocketGatewayServer.main(new String[]{flag}));
  }

  @Test
  void rejectsDuplicateFlagsBeforeConnectingToAeron() {
    assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.main(new String[]{"--port=8080", "--port=8081"}));
  }

  @Test
  void configHasLocalDemoDefaultsAndSupportsExplicitLimits() {
    assertEquals(new WebSocketGatewayServer.Config(8080, 128, 8), WebSocketGatewayServer.Config.parse(new String[0]));
    assertEquals(new WebSocketGatewayServer.Config(0, 16, 4),
        WebSocketGatewayServer.Config.parse(new String[]{"--port=0", "--queue-capacity=16", "--max-in-flight=4"}));
  }
}
