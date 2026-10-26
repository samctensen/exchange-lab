package dev.sam.exchange.audit;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/** Owns only this invocation's disposable Media Driver files, including partial startup files. */
final class InspectionWorkspace implements AutoCloseable {
  private final Path root = Files.createTempDirectory("exchange-inspection-");

  InspectionWorkspace() throws IOException {
  }

  Path driverDirectory() {
    return root.resolve("driver");
  }

  @Override
  public void close() throws IOException {
    if (Files.notExists(root))
      return;
    // Do not follow symbolic links: cleanup must stay within the directory we created.
    Files.walkFileTree(root, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path directory, IOException failure) throws IOException {
        if (failure != null)
          throw failure;
        Files.delete(directory);
        return FileVisitResult.CONTINUE;
      }
    });
  }
}
