package dev.sam.exchange.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import dev.sam.exchange.engine.CommandResult;
import dev.sam.exchange.engine.EngineCommand;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.RejectReason;
import dev.sam.exchange.engine.RejectResult;

// Applies requests and remembers responses; persistence is handled by the caller.
public class RequestStateMachine {
  private record CachedRequest(EngineCommand command, CommandResponse response) {
  }

  private final MatchingEngine engine;
  private final Map<UUID, CachedRequest> completedRequests = new HashMap<>();

  public RequestStateMachine(MatchingEngine engine) {
    this.engine = engine;
  }

  public boolean hasProcessed(UUID requestId) {
    return completedRequests.containsKey(requestId);
  }

  public CommandResponse process(CommandRequest request) {
    UUID requestId = request.requestId();
    CachedRequest cachedRequest = completedRequests.get(requestId);
    if (cachedRequest != null) {
      if (!cachedRequest.command().equals(request.command())) {
        return new CommandResponse(requestId,
            new RejectResult(request.command().orderId(), RejectReason.REQUEST_ID_CONFLICT));
      }
      return cachedRequest.response();
    }
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
