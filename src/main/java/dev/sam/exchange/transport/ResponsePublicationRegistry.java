package dev.sam.exchange.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;

public class ResponsePublicationRegistry implements AutoCloseable {
  private final Aeron aeron;
  private final Map<Long, Long> publicationRegistrations = new HashMap<>();
  private boolean closed;

  public ResponsePublicationRegistry(Aeron aeron) {
    this.aeron = aeron;
    this.closed = false;
  }

  public void register(long responseCorrelationId) {
    if (this.closed) {
      throw new IllegalStateException("Response publication registry is closed");
    }

    if (!publicationRegistrations.containsKey(responseCorrelationId)) {
      long registrationId = aeron
          .asyncAddPublication("aeron:ipc?control-mode=response|response-correlation-id=" + responseCorrelationId, 2);
      publicationRegistrations.put(responseCorrelationId, registrationId);
    }
  }

  public Optional<ConcurrentPublication> find(long responseCorrelationId) {
    Long registrationId = publicationRegistrations.get(responseCorrelationId);
    if (registrationId == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(aeron.getPublication(registrationId));
  }

  public void remove(long responseCorrelationId) {
    Long registrationId = publicationRegistrations.remove(responseCorrelationId);
    if (registrationId != null) {
      this.aeron.asyncRemovePublication(registrationId);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    publicationRegistrations.values().forEach(aeron::asyncRemovePublication);
    publicationRegistrations.clear();
  }
}
