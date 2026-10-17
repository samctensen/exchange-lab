package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.agrona.concurrent.SleepingIdleStrategy;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.sam.exchange.transport.AeronRequestClient;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

@Timeout(15)
class WebSocketHealthTest {
  @ParameterizedTest(name = "gateway started: {0}")
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("try") // Completing the WebSocket handshake proves /orders coexists without sending orders.
  void servesLiveAndActualReadinessWithoutPublishing(boolean started, @TempDir Path directory) throws Exception {
    try (
        MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .aeronDirectoryName(directory.resolve("driver").toString()).dirDeleteOnShutdown(true));
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        Subscription commands = aeron.addSubscription("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication requests = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(), 1);
        EngineGateway gateway = new EngineGateway(new AeronRequestClient(requests, replies), 1);
        WebSocketGateway server = new WebSocketGateway(gateway, 0)) {
      if (started) {
        gateway.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        var idle = new SleepingIdleStrategy();
        while (!gateway.isReady()) {
          assertTrue(System.nanoTime() - deadline < 0, "Gateway did not become ready");
          idle.idle();
        }
      }
      server.start();
      long position = requests.position();
      Response live = exchange(server.port(), "GET", "/health/live", "", "");
      assertResponse(live, "200 OK", "{\"status\":\"live\"}");
      Response ready = exchange(server.port(), "GET", "/health/ready", "", "");
      assertResponse(ready, started ? "200 OK" : "503 Service Unavailable",
          started ? "{\"status\":\"ready\"}" : "{\"status\":\"not_ready\"}");
      for (String path : new String[]{"/health/live", "/health/ready"}) {
        Response get = path.equals("/health/live") ? live : ready;
        Response head = exchange(server.port(), "HEAD", path, "", "");
        assertEquals(get.status(), head.status());
        assertEquals(get.headers().get("Content-Length"), head.headers().get("Content-Length"));
        assertEquals(get.headers().get("Content-Type"), head.headers().get("Content-Type"));
        assertEquals("", head.body());
      }
      try (var websocket = new WebSocketTestClient(server.port())) {
        assertEquals(position, requests.position(), "Probes and a WebSocket handshake must not publish commands");
      }
      assertEquals(0, commands.poll((buffer, offset, length, header) -> fail("Unexpected command"), 10));
      assertFalse(replies.isConnected(), "Health must not require or create a response publisher");
    }
  }

  @ParameterizedTest
  @MethodSource("malformedProbes")
  void rejectsMalformedHttpAndProbeBodies(String headers, String body, String status) throws Exception {
    try (EngineGateway gateway = new EngineGateway(new AeronRequestClient(null, null), 1);
        WebSocketGateway server = new WebSocketGateway(gateway, 0)) {
      server.start();
      Response response = exchange(server.port(), "GET", "/health/ready", headers, body);
      assertEquals("HTTP/1.1 " + status, response.status());
      assertEquals("", response.body());
      assertEquals("0", response.headers().get("Content-Length"));
      if (status.startsWith("400"))
        assertEquals("no-store", response.headers().get("Cache-Control"));
    }
  }

  private static Stream<Arguments> malformedProbes() {
    return Stream.of(Arguments.of("Content-Length: invalid\r\n", "", "400 Bad Request"),
        Arguments.of("Content-Length: -1\r\n", "", "400 Bad Request"),
        Arguments.of("Content-Length: 0\r\nContent-Length: 2\r\n", "{}", "400 Bad Request"),
        Arguments.of("Content-Length: 2\r\n", "{}", "400 Bad Request"),
        Arguments.of("Transfer-Encoding: chunked\r\n", "2\r\n{}\r\n0\r\n\r\n", "400 Bad Request"),
        Arguments.of("Connection: close\r\nContent-Length: 16385\r\n", "", "413 Request Entity Too Large"));
  }

  private static void assertResponse(Response response, String status, String body) {
    assertEquals("HTTP/1.1 " + status, response.status());
    assertEquals("application/json; charset=utf-8", response.headers().get("Content-Type"));
    assertEquals(Integer.toString(body.getBytes(StandardCharsets.UTF_8).length),
        response.headers().get("Content-Length"));
    assertEquals("close", response.headers().get("Connection"));
    assertEquals("no-store", response.headers().get("Cache-Control"));
    assertEquals(body, response.body());
  }

  static Response exchange(int port, String method, String path, String headers, String body) throws Exception {
    try (Socket socket = new Socket("127.0.0.1", port)) {
      socket.setSoTimeout(3000);
      String request = method + " " + path + " HTTP/1.1\r\nHost: 127.0.0.1:" + port + "\r\nConnection: keep-alive\r\n"
          + headers + "\r\n" + body;
      socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
      // Reading to EOF also verifies that probes promptly free their connection slot.
      String raw = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      String[] parts = raw.split("\r\n\r\n", 2);
      String[] lines = parts[0].split("\r\n");
      Map<String, String> responseHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (int i = 1; i < lines.length; i++) {
        String[] header = lines[i].split(":", 2);
        responseHeaders.put(header[0], header[1].trim());
      }
      return new Response(lines[0], responseHeaders, parts.length == 2 ? parts[1] : "");
    }
  }

  record Response(String status, Map<String, String> headers, String body) {
  }
}
