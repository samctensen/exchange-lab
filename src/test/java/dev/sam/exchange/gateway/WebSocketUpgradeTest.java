package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;

@Timeout(15)
class WebSocketUpgradeTest {
  @ParameterizedTest
  @CsvSource({"http://localhost:3000, https://example.com", "https://example.com, http://localhost:3000",
      "http://localhost:3000, http://localhost:3000"})
  void rejectsMultipleOriginHeaders(String firstOrigin, String secondOrigin) throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertEquals("HTTP/1.1 403 Forbidden",
          status(fixture.server.port(), "Origin: " + firstOrigin + "\r\norigin: " + secondOrigin + "\r\n"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"http://localhost:65536", "https://127.0.0.1:65536", "http://localhost:2147483647",
      "http://localhost:", "https://127.0.0.1:", "http://localhost:-1", "http://localhost:abc",
      "http://localhost:2147483648"})
  void rejectsMalformedOriginPorts(String origin) throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertEquals("HTTP/1.1 403 Forbidden", status(fixture.server.port(), "Origin: " + origin + "\r\n"));
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"http://localhost", "https://127.0.0.1", "http://localhost:0", "https://localhost:65535",
      "http://127.0.0.1:0", "https://127.0.0.1:65535"})
  void acceptsNativeClientsAndValidLocalOriginPorts(String origin) throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertEquals("HTTP/1.1 101 Switching Protocols",
          status(fixture.server.port(), origin == null ? "" : "Origin: " + origin + "\r\n"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"POST", "PUT", "DELETE", "HEAD", "OPTIONS"})
  void ordersEndpointRequiresGet(String method) throws Exception {
    try (Fixture fixture = new Fixture()) {
      Response response = response(fixture.server.port(), method, "/orders", "");
      assertEquals("HTTP/1.1 405 Method Not Allowed", response.status());
      assertEquals("GET", response.headers().get("Allow"));
    }
  }

  @ParameterizedTest
  @CsvSource({"GET, /wrong", "POST, /wrong", "GET, /orders?query=1", "POST, /orders/"})
  void unknownPathsRemainNotFound(String method, String path) throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertEquals("HTTP/1.1 404 Not Found", response(fixture.server.port(), method, path, "").status());
    }
  }

  @ParameterizedTest
  @CsvSource({"GET, /wrong, , 404 Not Found", "POST, /orders, , 405 Method Not Allowed",
      "GET, /orders, Origin: https://example.com, 403 Forbidden",
      "GET, /orders, Content-Length: invalid, 400 Bad Request"})
  void rejectedUpgradesDeclareAnEmptyBodyAndCloseTheConnection(String method, String path, String header,
      String expectedStatus) throws Exception {
    try (Fixture fixture = new Fixture()) {
      Response response = response(fixture.server.port(), method, path, header == null ? "" : header + "\r\n");
      assertEquals("HTTP/1.1 " + expectedStatus, response.status());
      assertEquals("0", response.headers().get("Content-Length"));
      assertEquals("close", response.headers().get("Connection"));
    }
  }

  private static String status(int port, String headers) throws Exception {
    return response(port, "GET", "/orders", headers).status();
  }

  private static Response response(int port, String method, String path, String headers) throws Exception {
    try (Socket socket = new Socket("127.0.0.1", port)) {
      socket.setSoTimeout(3000);
      String request = method + " " + path + " HTTP/1.1\r\nHost: 127.0.0.1:" + port
          + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
          + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" + headers + "\r\n";
      socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
      var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
      String status = reader.readLine();
      Map<String, String> responseHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (String line; (line = reader.readLine()) != null && !line.isEmpty();) {
        String[] header = line.split(":", 2);
        responseHeaders.put(header[0], header[1].trim());
      }
      if (!"HTTP/1.1 101 Switching Protocols".equals(status))
        assertEquals(-1, reader.read(), "Rejected upgrade must close without sending a response body");
      return new Response(status, responseHeaders);
    }
  }

  private record Response(String status, Map<String, String> headers) {
  }

  private static class Fixture implements AutoCloseable {
    final EngineGateway gateway = new EngineGateway(
        new ReplyingAeronClient(request -> new CancelResult(request.command().orderId(), false)), 1, 1);
    final WebSocketGateway server = new WebSocketGateway(gateway, 0);

    Fixture() throws Exception {
      try {
        server.start();
      } catch (Exception | Error failure) {
        close();
        throw failure;
      }
    }

    @Override
    public void close() {
      server.close();
      gateway.close();
    }
  }
}
