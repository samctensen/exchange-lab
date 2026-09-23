package dev.sam.exchange.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ArchiveRuntimeTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void refusesPersistentStorageInsideDisposableDriverDirectory(boolean nested, @TempDir Path directory)
      throws Exception {
    Path driver = directory.resolve("aeron");
    Path archive = nested ? driver.resolve("archive") : driver;
    Files.createDirectories(archive);
    Path saved = Files.writeString(archive.resolve("existing-data"), "must survive");
    assertThrows(IllegalArgumentException.class, () -> {
      try (ArchiveRuntime runtime = ArchiveRuntime.launch(archive, driver.toString())) {
        fail("Accepted Archive storage that the driver would delete on shutdown: " + runtime.archive().archiveId());
      }
    });
    assertEquals("must survive", Files.readString(saved));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checksRealPathsBeforeStartingTheDriver(boolean nested, @TempDir Path directory) throws Exception {
    Path driver = Files.createDirectories(directory.resolve("aeron"));
    Path alias = Files.createSymbolicLink(directory.resolve("alias"), driver);
    Path archive = nested ? alias.resolve("archive") : alias;
    assertThrows(IllegalArgumentException.class, () -> {
      try (ArchiveRuntime runtime = ArchiveRuntime.launch(archive, driver.toString())) {
        fail("Accepted aliased driver storage: " + runtime.archive().archiveId());
      }
    });
  }
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checksPhysicalContainmentForCaseAliasesOfNewDirectories(boolean nested, @TempDir Path directory)
      throws Exception {
    Files.createDirectory(directory.resolve("case-probe"));
    assumeTrue(Files.exists(directory.resolve("CASE-PROBE")), "Requires a case-insensitive filesystem");
    Path driver = directory.resolve("aeron");
    Path alias = directory.resolve("AERON");
    Path archive = nested ? alias.resolve("archive") : alias;
    assertThrows(IllegalArgumentException.class, () -> {
      try (ArchiveRuntime runtime = ArchiveRuntime.launch(archive, driver.toString())) {
        fail("Accepted a case alias of disposable driver storage: " + runtime.archive().archiveId());
      }
    });
  }

}
