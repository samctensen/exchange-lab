package dev.sam.exchange.audit;

import java.util.Objects;

import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.transport.RequestStateMachine;
import io.aeron.archive.client.AeronArchive;

/** Replays stopped request history into fresh matching state without appending requests. */
public final class ArchiveInspector {
  private ArchiveInspector() {
  }

  /** Borrows the caller's Archive client. The caller retains ownership and must serialize access. */
  public static ArchiveInspection inspect(AeronArchive archive, int levels) {
    Objects.requireNonNull(archive, "archive");
    ArchiveInspectorConfig.validateLevels(levels);
    checkInterrupted();
    OrderBook book = new OrderBook();
    ReplayAudit audit = new ReplayAudit(new RequestStateMachine(new MatchingEngine(book)));
    ArchiveRequestLog.replay(archive, audit);
    ArchiveInspection result = new ArchiveInspection(audit.snapshot(), book.summary(), book.depth(levels));
    checkInterrupted();
    return result;
  }

  private static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted())
      throw new IllegalStateException("Archive inspection interrupted");
  }
}
