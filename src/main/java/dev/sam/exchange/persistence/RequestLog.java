package dev.sam.exchange.persistence;

import dev.sam.exchange.transport.CommandRequest;

/** The engine thread offers requests and waits for their returned end positions before applying them. */
public interface RequestLog {
  /** Return a nonnegative end position on acceptance, or a negative Aeron status when the offer fails. */
  long offer(CommandRequest request);

  /** Check recording progress for a nonnegative position; negative offer statuses are invalid positions. */
  boolean isRecorded(long position);
}
