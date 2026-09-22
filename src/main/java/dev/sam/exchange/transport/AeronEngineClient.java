package dev.sam.exchange.transport;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

public class AeronEngineClient {

  public static void main(String[] args) {
    // Connect to the media driver owned by the server.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The ask partially fills the resting bid.
    List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L));

    // Send requests on stream 1 and receive correlated responses on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Publication publication = aeron.addPublication("aeron:ipc", 1);
        Subscription replies = aeron.addSubscription("aeron:ipc", 2)) {

      AeronRequestClient client = new AeronRequestClient(publication, replies);

      for (EngineCommand order : orders) {
        CommandRequest request = new CommandRequest(UUID.randomUUID(), order);
        System.out.println(client.send(request));
      }
    }
  }
}
