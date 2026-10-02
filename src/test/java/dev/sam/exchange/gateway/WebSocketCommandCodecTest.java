package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonParser;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.CancelResult;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.engine.Trade;
import dev.sam.exchange.transport.CommandRequest;

class WebSocketCommandCodecTest {
  private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private final WebSocketCommandCodec codec = new WebSocketCommandCodec();

  @Test
  void decodesPlaceWithoutLosingLongPrecision() {
    String json = """
        {"requestId":"00000000-0000-0000-0000-000000000001",
         "place":{"orderId":"9223372036854775807","side":"BID",
                  "priceTicks":"9007199254740993","quantityLots":"10"}}
        """;
    assertEquals(new CommandRequest(ID, new PlaceOrder(Long.MAX_VALUE, Side.BID, 9007199254740993L, 10)),
        codec.decode(json));
  }

  @Test
  void acceptsEitherFieldOrderAndPreservesSignedOrderIdentifiers() {
    assertEquals(new CommandRequest(ID, new CancelOrder(Long.MIN_VALUE)),
        codec.decode("{\"cancel\":{\"orderId\":\"-9223372036854775808\"},\"requestId\":\"" + ID + "\"}"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "null", "[]", "{requestId:'bad'}",
      "{\"requestId\":\"1-1-1-1-1\",\"cancel\":{\"orderId\":\"1\"}}",
      "{\"requestId\":null,\"cancel\":{\"orderId\":\"1\"}}", "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":1}}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1.5\"}}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"9223372036854775808\"}}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\",\"orderId\":\"2\"}}",
      "{\"requestId\":\"ID\",\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\"}}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\"},\"unknown\":true}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\",\"unknown\":\"2\"}}", "{\"requestId\":\"ID\",\"cancel\":{}}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\"}} {}",
      "{\"requestId\":\"ID\",\"cancel\":{\"orderId\":\"1\"},\"place\":{}}",
      "{\"requestId\":\"ID\",\"place\":{\"orderId\":\"1\",\"side\":\"BUY\",\"priceTicks\":\"1\",\"quantityLots\":\"1\"}}",
      "{\"requestId\":\"ID\",\"place\":{\"orderId\":\"1\",\"side\":\"ASK\",\"priceTicks\":\"0\",\"quantityLots\":\"1\"}}",
      "{\"requestId\":\"ID\",\"place\":{\"orderId\":\"1\",\"side\":\"ASK\",\"priceTicks\":\"1\",\"quantityLots\":\"-1\"}}"})
  void rejectsMalformedAmbiguousOrInvalidCommands(String json) {
    assertThrows(IllegalArgumentException.class, () -> codec.decode(json.replace("ID", ID.toString())));
  }

  @Test
  void encodesResultsIncludingTradesAsStringLongs() {
    var json = JsonParser
        .parseString(
            codec.encode(ID, new PlaceResult(Long.MAX_VALUE, List.of(new Trade(Long.MAX_VALUE, 2, 100, 4)), 6)))
        .getAsJsonObject();
    assertEquals(ID.toString(), json.get("requestId").getAsString());
    var place = json.getAsJsonObject("place");
    assertTrue(place.getAsJsonPrimitive("orderId").isString());
    assertEquals("9223372036854775807", place.get("orderId").getAsString());
    assertEquals("6", place.get("remainingLots").getAsString());
    assertEquals(JsonParser.parseString("""
        [{"incomingOrderId":"9223372036854775807","restingOrderId":"2","priceTicks":"100","quantityLots":"4"}]
        """), place.get("trades"));
  }

  @Test
  void encodesCancelFalseAndBusinessRejectionsExplicitly() {
    var cancelled = JsonParser.parseString(codec.encode(ID, new CancelResult(1, false))).getAsJsonObject();
    assertFalse(cancelled.getAsJsonObject("cancel").get("cancelled").getAsBoolean());
    var rejected = JsonParser.parseString(codec.encode(ID, new RejectResult(1, RejectReason.REQUEST_ID_CONFLICT)))
        .getAsJsonObject();
    assertEquals("REQUEST_ID_CONFLICT", rejected.getAsJsonObject("reject").get("reason").getAsString());
  }

  @Test
  void errorMessagesEscapeContentAndPreserveOutcomeUncertainty() {
    var error = JsonParser.parseString(codec.error(null, "INVALID_ARGUMENT", "bad \"value\"\n", false))
        .getAsJsonObject();
    assertTrue(error.get("requestId").isJsonNull());
    assertEquals("bad \"value\"\n", error.getAsJsonObject("error").get("message").getAsString());
    assertFalse(error.getAsJsonObject("error").get("outcomeUnknown").getAsBoolean());
  }
}
