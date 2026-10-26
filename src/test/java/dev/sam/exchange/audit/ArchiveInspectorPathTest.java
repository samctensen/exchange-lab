package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;

@Timeout(30)
class ArchiveInspectorPathTest {
  @TempDir
  Path directory;

  @Test
  void existingHistoryCanBeInspectedTwiceAndReopenedWithoutExtraRequests() throws Exception {
    Path archive = directory.resolve("archive");
    long id;
    long stop;
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(archive, directory.resolve("writer").toString())) {
      try (ArchiveRequestLog log = ArchiveRequestLog.open(runtime.archive(), ignored -> {
      })) {
        id = log.recordingId();
        ArchiveInspectionTest.append(log, ArchiveInspectionTest.history());
      }
      stop = runtime.archive().getStopPosition(id);
    }
    var first = ArchiveInspector.inspect(archive, 1);
    ArchiveInspectionTest.assertHistory(first);
    assertEquals(first, ArchiveInspector.inspect(archive, 1));
    try (ArchiveRuntime reopened = ArchiveRuntime.launch(archive, directory.resolve("reader").toString())) {
      assertEquals(stop, reopened.archive().getStopPosition(id));
      assertEquals(first, ArchiveInspector.inspect(reopened.archive(), 1));
    }
  }

  @Test
  void rejectsMissingAndUninitializedDirectoriesWithoutCreatingHistory() throws Exception {
    Path missing = directory.resolve("typo");
    assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(missing, 10));
    assertFalse(Files.exists(missing));
    Path uninitialized = Files.createDirectory(directory.resolve("uninitialized"));
    Files.writeString(uninitialized.resolve("keep.txt"), "keep me");
    assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(uninitialized, 10));
    assertFalse(Files.exists(uninitialized.resolve("archive.catalog")));
    assertEquals("keep me", Files.readString(uninitialized.resolve("keep.txt")));
    assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(uninitialized.resolve("keep.txt"), 10));
  }

  @Test
  void invalidLimitsAndPreexistingInterruptionNeverOpenResources() throws Exception {
    Path missing = directory.resolve("missing");
    var error = assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(missing, 0));
    assertTrue(error.getMessage().contains("--levels"));
    assertThrows(IllegalArgumentException.class, () -> ArchiveInspector.inspect(Path.of(""), 1));
    Thread.currentThread().interrupt();
    try {
      assertThrows(IllegalStateException.class, () -> ArchiveInspector.inspect(missing, 1));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    assertFalse(Files.exists(missing));
  }
}
