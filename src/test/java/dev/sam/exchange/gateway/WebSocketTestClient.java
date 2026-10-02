package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

final class WebSocketTestClient implements WebSocket.Listener, AutoCloseable {
  private final HttpClient http = HttpClient.newHttpClient();
  private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
  private final StringBuilder fragments = new StringBuilder();
  final CompletableFuture<Integer> closed = new CompletableFuture<>();
  final WebSocket socket;

  WebSocketTestClient(int port) throws Exception {
    try {
      socket = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(3))
          .buildAsync(URI.create("ws://127.0.0.1:" + port + "/orders"), this).get(3, TimeUnit.SECONDS);
    } catch (Exception | Error e) {
      http.close();
      throw e;
    }
  }

  @Override
  public void onOpen(WebSocket socket) {
    socket.request(1);
  }

  @Override
  public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
    fragments.append(data);
    if (last) {
      messages.add(fragments.toString());
      fragments.setLength(0);
    }
    socket.request(1);
    return null;
  }

  @Override
  public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
    closed.complete(statusCode);
    return null;
  }

  @Override
  public void onError(WebSocket socket, Throwable error) {
    closed.completeExceptionally(error);
  }

  JsonObject request(String text) throws Exception {
    socket.sendText(text, true).get(3, TimeUnit.SECONDS);
    return response();
  }

  JsonObject response() throws Exception {
    String response = messages.poll(5, TimeUnit.SECONDS);
    assertNotNull(response, "No WebSocket response received");
    return JsonParser.parseString(response).getAsJsonObject();
  }

  @Override
  public void close() {
    socket.abort();
    http.close();
  }
}
