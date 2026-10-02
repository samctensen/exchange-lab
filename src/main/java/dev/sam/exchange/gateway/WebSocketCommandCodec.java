package dev.sam.exchange.gateway;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.transport.CommandRequest;

/** Browser-facing JSON; the internal Aeron protocol remains SBE. */
public final class WebSocketCommandCodec {
  public CommandRequest decode(String text) {
    try (JsonReader reader = new JsonReader(new StringReader(text))) {
      reader.setStrictness(Strictness.STRICT);
      reader.beginObject();
      var fields = new HashSet<String>();
      UUID requestId = null;
      EngineCommand command = null;
      while (reader.hasNext()) {
        String name = reader.nextName();
        if (!fields.add(name))
          throw new IllegalArgumentException("Duplicate field: " + name);
        switch (name) {
          case "requestId" -> {
            String value = string(reader);
            requestId = UUID.fromString(value);
            if (!requestId.toString().equalsIgnoreCase(value))
              throw new IllegalArgumentException("requestId must be a full UUID");
          }
          case "place", "cancel" -> {
            if (command != null)
              throw new IllegalArgumentException("Exactly one command is required");
            command = command(reader, name);
          }
          default -> throw new IllegalArgumentException("Unknown field: " + name);
        }
      }
      reader.endObject();
      if (reader.peek() != JsonToken.END_DOCUMENT || requestId == null || command == null)
        throw new IllegalArgumentException("Expected a requestId and one command");
      return new CommandRequest(requestId, command);
    } catch (IOException | IllegalStateException e) {
      throw new IllegalArgumentException("Invalid JSON command", e);
    }
  }

  private EngineCommand command(JsonReader reader, String type) throws IOException {
    Set<String> expected = type.equals("place")
        ? Set.of("orderId", "side", "priceTicks", "quantityLots")
        : Set.of("orderId");
    Map<String, String> fields = new HashMap<>();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (!expected.contains(name) || fields.containsKey(name))
        throw new IllegalArgumentException("Unknown or duplicate command field: " + name);
      fields.put(name, string(reader));
    }
    reader.endObject();
    if (!fields.keySet().equals(expected))
      throw new IllegalArgumentException("Missing command fields");
    long orderId = Long.parseLong(fields.get("orderId"));
    return type.equals("cancel")
        ? new CancelOrder(orderId)
        : new PlaceOrder(orderId, Side.valueOf(fields.get("side")), Long.parseLong(fields.get("priceTicks")),
            Long.parseLong(fields.get("quantityLots")));
  }

  private String string(JsonReader reader) throws IOException {
    if (reader.peek() != JsonToken.STRING)
      throw new IllegalArgumentException("Fields must be strings; encode 64-bit integers as decimal strings");
    return reader.nextString();
  }

  public String encode(UUID requestId, CommandResult result) {
    JsonObject root = envelope(requestId);
    JsonObject body = new JsonObject();
    switch (result) {
      case dev.sam.exchange.engine.PlaceResult place -> {
        body.addProperty("orderId", Long.toString(place.orderId()));
        body.addProperty("remainingLots", Long.toString(place.remainingLots()));
        JsonArray trades = new JsonArray();
        for (var trade : place.trades()) {
          JsonObject item = new JsonObject();
          item.addProperty("incomingOrderId", Long.toString(trade.incomingOrderId()));
          item.addProperty("restingOrderId", Long.toString(trade.restingOrderId()));
          item.addProperty("priceTicks", Long.toString(trade.priceTicks()));
          item.addProperty("quantityLots", Long.toString(trade.quantityLots()));
          trades.add(item);
        }
        body.add("trades", trades);
        root.add("place", body);
      }
      case dev.sam.exchange.engine.CancelResult cancel -> {
        body.addProperty("orderId", Long.toString(cancel.orderId()));
        body.addProperty("cancelled", cancel.cancelled());
        root.add("cancel", body);
      }
      case dev.sam.exchange.engine.RejectResult reject -> {
        body.addProperty("orderId", Long.toString(reject.orderId()));
        body.addProperty("reason", reject.reason().name());
        root.add("reject", body);
      }
    }
    return root.toString();
  }

  public String error(UUID requestId, String code, String message, boolean outcomeUnknown) {
    JsonObject root = envelope(requestId);
    JsonObject error = new JsonObject();
    error.addProperty("code", code);
    error.addProperty("message", message);
    error.addProperty("outcomeUnknown", outcomeUnknown);
    root.add("error", error);
    return root.toString();
  }

  private JsonObject envelope(UUID requestId) {
    JsonObject root = new JsonObject();
    if (requestId == null)
      root.add("requestId", JsonNull.INSTANCE);
    else
      root.addProperty("requestId", requestId.toString());
    return root;
  }
}
