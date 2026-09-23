package dev.sam.exchange.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;

/** Owns the live server's embedded driver and persistent Archive. */
public final class ArchiveRuntime implements AutoCloseable {
  private final ArchivingMediaDriver driver;
  private final AeronArchive archive;

  private ArchiveRuntime(ArchivingMediaDriver driver, AeronArchive archive) {
    this.driver = driver;
    this.archive = archive;
  }

  public static ArchiveRuntime launch(Path directory, String aeronDirectory) throws IOException {
    Path archivePath = directory.toFile().getCanonicalFile().toPath();
    Path driverPath = Path.of(aeronDirectory).toFile().getCanonicalFile().toPath();
    if (archivePath.startsWith(driverPath)) {
      throw new IllegalArgumentException("Archive directory must be outside the disposable Aeron driver directory");
    }
    Files.createDirectories(archivePath);
    // Newly created paths can be case aliases on macOS. Compare filesystem identity before the
    // driver gets a chance to delete its directory, including aliases that canonical text missed.
    if (Files.exists(driverPath)) {
      for (Path ancestor = archivePath.toRealPath(); ancestor != null; ancestor = ancestor.getParent()) {
        if (Files.isSameFile(ancestor, driverPath)) {
          throw new IllegalArgumentException("Archive directory must be outside the disposable Aeron driver directory");
        }
      }
    }
    ArchivingMediaDriver driver = ArchivingMediaDriver.launch(
        new MediaDriver.Context().aeronDirectoryName(driverPath.toString()).dirDeleteOnShutdown(true),
        new Archive.Context().archiveDir(archivePath.toFile()).deleteArchiveOnStart(false).controlChannelEnabled(false)
            .localControlChannel("aeron:ipc").recordingEventsEnabled(false)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:ipc"))
            // The recording counter advances after forcing recorded data and metadata to storage.
            .fileSyncLevel(2).catalogFileSyncLevel(2));
    try {
      AeronArchive archive = AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(driverPath.toString())
          .controlRequestChannel("aeron:ipc").controlRequestStreamId(driver.archive().context().localControlStreamId())
          .controlResponseChannel("aeron:ipc"));
      return new ArchiveRuntime(driver, archive);
    } catch (RuntimeException | Error failure) {
      driver.close();
      throw failure;
    }
  }

  public AeronArchive archive() {
    return archive;
  }

  @Override
  public void close() {
    try {
      archive.close();
    } finally {
      driver.close();
    }
  }
}
