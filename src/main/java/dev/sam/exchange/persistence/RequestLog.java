package dev.sam.exchange.persistence;

import dev.sam.exchange.transport.CommandRequest;

/** The engine thread offers one request, then waits for its returned end position to be recorded. */
public interface RequestLog {
  long offer(CommandRequest request);

  boolean isRecorded(long position);
}
