package dev.sam.exchange.audit;

import java.util.Objects;

import dev.sam.exchange.engine.BookDepth;
import dev.sam.exchange.engine.BookSummary;

/** Aggregate results captured only after a complete replay into a fresh state machine. */
public record ArchiveInspection(ReplayAuditSnapshot audit, BookSummary book, BookDepth depth) {
  public ArchiveInspection {
    Objects.requireNonNull(audit, "audit");
    Objects.requireNonNull(book, "book");
    Objects.requireNonNull(depth, "depth");
  }
}
