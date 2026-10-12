package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  private static String status(int port, String headers) throws Exception {
    try (Socket socket = new Socket("127.0.0.1", port)) {
      socket.setSoTimeout(3000);
      String request = "GET /orders HTTP/1.1\r\nHost: 127.0.0.1:" + port
          + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
          + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" + headers + "\r\n";
      socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
      var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
      return reader.readLine();
    }
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
