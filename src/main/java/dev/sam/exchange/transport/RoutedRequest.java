package dev.sam.exchange.transport;

import java.util.Objects;

public record RoutedRequest(CommandRequest request, long responseCorrelationId) {

  public RoutedRequest {
    Objects.requireNonNull(request, "request");
  }
}
