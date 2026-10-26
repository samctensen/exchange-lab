package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ArchiveInspectorConfigTest {
  @Test
  void requiresAnExplicitArchiveAndDefaultsToTenTextLevels() {
    assertThrows(IllegalArgumentException.class, () -> ArchiveInspectorConfig.parse(new String[0]));
    var config = ArchiveInspectorConfig.parse(new String[]{"--archive=stored history"});
    assertEquals(Path.of("stored history"), config.archive());
    assertEquals(10, config.levels());
    assertEquals(ArchiveInspectorConfig.Format.TEXT, config.format());
  }

  @Test
  void preservesAPathContainingEqualsAndAcceptsReorderedOptions() {
    var config = ArchiveInspectorConfig.parse(new String[]{"--format=json", "--levels=1000", "--archive=data=a"});
    assertEquals(Path.of("data=a"), config.archive());
    assertEquals(1000, config.levels());
    assertEquals(ArchiveInspectorConfig.Format.JSON, config.format());
    assertEquals(1, ArchiveInspectorConfig.parse(new String[]{"--archive=a", "--levels=1"}).levels());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--levels=0", "--levels=-1", "--levels=1001", "--levels=2147483648", "--levels=oops",
      "--levels=", "--format=csv", "--format=JSON", "--format=", "--archive=", "--archive=  ", "--archive", "--levels",
      "--format", "--unknown=1", "positional"})
  void invalidArgumentsIdentifyTheOffendingOption(String flag) {
    String[] args = flag.startsWith("--archive") ? new String[]{flag} : new String[]{"--archive=a", flag};
    var failure = assertThrows(IllegalArgumentException.class, () -> ArchiveInspectorConfig.parse(args));
    assertTrue(failure.getMessage().contains(flag.split("=", 2)[0]), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--archive=a", "--levels=10", "--format=text"})
  void duplicateOptionsAreRejected(String flag) {
    var failure = assertThrows(IllegalArgumentException.class,
        () -> ArchiveInspectorConfig.parse(new String[]{flag, flag}));
    assertTrue(failure.getMessage().contains("Duplicate"));
  }

  @Test
  void invalidDirectConfigurationsCannotBypassTheParser() {
    assertThrows(NullPointerException.class,
        () -> new ArchiveInspectorConfig(null, 10, ArchiveInspectorConfig.Format.TEXT));
    assertThrows(NullPointerException.class, () -> new ArchiveInspectorConfig(Path.of("a"), 10, null));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveInspectorConfig(Path.of("a"), 0, ArchiveInspectorConfig.Format.TEXT));
    assertThrows(IllegalArgumentException.class,
        () -> new ArchiveInspectorConfig(Path.of("a"), 1001, ArchiveInspectorConfig.Format.TEXT));
  }
}
