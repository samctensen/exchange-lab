package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AeronEngineClientCliTest {
  @Test
  void rejectsInvalidArgumentsBeforeConnecting(@TempDir Path tempDir) {
    String originalTempDir = System.getProperty("java.io.tmpdir");
    System.setProperty("java.io.tmpdir", tempDir.toString());
    try {
      for (String[] args : List.of(new String[]{"--unknown=value"}, new String[]{"unexpected"},
          new String[]{"--aeron-directory"}, new String[]{"--aeron-directory="}, new String[]{"--aeron-directory=   "},
          new String[]{"--aeron-directory=invalid\0path"},
          new String[]{"--aeron-directory=" + tempDir, "--aeron-directory=" + tempDir})) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> AeronEngineClient.main(args), String.join(" ", args));
        assertTrue(error.getMessage().contains(args[0].split("=", 2)[0]), error::getMessage);
        assertFalse(Files.exists(tempDir.resolve("exchange-lab-aeron")),
            "Invalid arguments must not create driver resources");
      }
    } finally {
      System.setProperty("java.io.tmpdir", originalTempDir);
    }
  }
}
