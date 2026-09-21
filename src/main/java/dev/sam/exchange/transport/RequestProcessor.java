package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;
import dev.sam.exchange.persistence.RequestJournal;

public class RequestProcessor {
  private record CachedRequest(EngineCommand command, CommandResponse response) {
  }

  private final MatchingEngine engine;
  private final RequestJournal journal;
  private final Map<UUID, CachedRequest> completedRequests;

  public RequestProcessor(MatchingEngine engine, RequestJournal journal) {
    this.engine = engine;
    this.journal = journal;
    this.completedRequests = new HashMap<>();
  }

  public static RequestProcessor recover(Path path) throws IOException {
    RequestJournal journal = new RequestJournal(path);
    OrderBook orderBook = new OrderBook();

    MatchingEngine engine = new MatchingEngine(orderBook);

    RequestProcessor processor = new RequestProcessor(engine, journal);
    List<CommandRequest> requests = journal.readAll();

    for (CommandRequest request : requests) {
      if (processor.completedRequests.containsKey(request.requestId())) {
        throw new IOException("Duplicate request ID in journal: " + request.requestId());
      }
      processor.apply(request);
    }
    return processor;
  }

  public CommandResponse process(CommandRequest request) throws IOException {
    UUID requestId = request.requestId();
    CachedRequest cachedRequest = completedRequests.get(requestId);
    if (cachedRequest != null) {
      if (!cachedRequest.command().equals(request.command())) {
        return new CommandResponse(requestId,
            new RejectResult(request.command().orderId(), RejectReason.REQUEST_ID_CONFLICT));
      }
      return cachedRequest.response();
    }
    journal.append(request);
    return apply(request);
  }

  private CommandResponse apply(CommandRequest request) {
    var rejection = engine.validate(request.command());
    CommandResult result = rejection.isPresent() ? rejection.get() : engine.process(request.command());
    CommandResponse response = new CommandResponse(request.requestId(), result);
    completedRequests.put(request.requestId(), new CachedRequest(request.command(), response));

    return response;
  }
}
