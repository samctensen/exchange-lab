package dev.sam.exchange.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.gateway.EngineGateway;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

public class AeronEngineClient {

  public static void main(String[] args) {
    if (Arrays.asList(args).contains("--help")) {
      if (args.length != 1) {
        throw new IllegalArgumentException("--help must be used alone");
      }
      System.out.println(EngineClientConfig.help());
      return;
    }

    // Connect to the media driver owned by the server.
    EngineClientConfig config = EngineClientConfig.parse(args);

    // The ask partially fills the resting bid.
    List<EngineCommand> orders = List.of(new PlaceOrder(1L, Side.BID, 100L, 10L),
        new PlaceOrder(2L, Side.ASK, 99L, 4L));

    // Send requests on stream 1 and receive correlated responses on stream 2.
    try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDirectory().toString()));
        Subscription replies = aeron.addSubscription("aeron:ipc?control-mode=response", 2);
        Publication publication = aeron.addPublication("aeron:ipc?response-correlation-id=" + replies.registrationId(),
            1)) {

      AeronRequestClient client = new AeronRequestClient(publication, replies, config.clientConfig());

      try (EngineGateway gateway = new EngineGateway(client, 128)) {
        gateway.start();

        List<CompletableFuture<CommandResult>> results = new ArrayList<>();

        // Submit every order and collect its future.
        for (EngineCommand order : orders) {
          CommandRequest request = new CommandRequest(UUID.randomUUID(), order);
          results.add(gateway.submit(request));
        }

        // Then wait for each result and print it.
        for (CompletableFuture<CommandResult> result : results) {
          System.out.println(result.join());
        }
      }
    }
  }
}
