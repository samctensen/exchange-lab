package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionWorkspaceTest {
  @TempDir
  Path directory;

  @Test
  void cleansOwnedFilesAfterFailureWithoutFollowingLinksOutsideWorkspace() throws Exception {
    Path untouched = Files.writeString(directory.resolve("untouched"), "keep");
    var workspace = new InspectionWorkspace();
    Path root = workspace.driverDirectory().getParent();
    IOException original = new IOException("startup failed");
    assertSame(original, assertThrows(IOException.class, () -> {
      try (workspace) {
        Files.createDirectories(workspace.driverDirectory().resolve("nested"));
        Files.writeString(workspace.driverDirectory().resolve("nested/cnc.dat"), "partial driver");
        Files.createSymbolicLink(root.resolve("outside"), directory);
        throw original;
      }
    }));
    assertFalse(Files.exists(root));
    assertEquals("keep", Files.readString(untouched));
    workspace.close();
  }
}
