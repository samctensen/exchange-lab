package dev.sam.exchange.audit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.transport.RequestStateMachine;
import io.aeron.archive.client.AeronArchive;

/** Replays stopped request history into fresh matching state without appending requests. */
public final class ArchiveInspector {
  private ArchiveInspector() {
  }

  /**
   * Opens an existing, offline Archive with a private Media Driver. Startup can update Archive
   * housekeeping metadata; this method never appends request history. Stop the existing owner first.
   */
  public static ArchiveInspection inspect(Path directory, int levels) throws IOException {
    new ArchiveInspectorConfig(directory, levels, ArchiveInspectorConfig.Format.TEXT);
    checkInterrupted();
    if (!Files.isDirectory(directory) || !Files.isRegularFile(directory.resolve("archive.catalog")))
      throw new IllegalArgumentException(
          "--archive must name an existing Aeron Archive directory with archive.catalog");
    Path archive = directory.toRealPath();
    ArchiveInspection result;
    try (InspectionWorkspace workspace = new InspectionWorkspace();
        ArchiveRuntime runtime = ArchiveRuntime.launch(archive, workspace.driverDirectory().toString())) {
      result = inspect(runtime.archive(), levels);
    }
    checkInterrupted();
    return result;
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
