package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AeronEngineClientCliTest {
  @Test
  void helpExplainsDriverSelectionWithoutConnecting(@TempDir Path tempDir) {
    String originalTempDir = System.getProperty("java.io.tmpdir");
    PrintStream originalOut = System.out;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    System.setProperty("java.io.tmpdir", tempDir.toString());
    try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      assertDoesNotThrow(() -> AeronEngineClient.main(new String[]{"--help"}));
      String help = output.toString(StandardCharsets.UTF_8);
      assertTrue(help.contains("Usage: AeronEngineClient"), help);
      assertTrue(help.contains("AeronEngineServer"), help);
      assertTrue(help.contains("--aeron-directory=path"), help);
      assertTrue(help.contains("java.io.tmpdir/exchange-lab-aeron"), help);
      assertFalse(Files.exists(tempDir.resolve("exchange-lab-aeron")), "Help must not create driver resources");
    } finally {
      System.setOut(originalOut);
      System.setProperty("java.io.tmpdir", originalTempDir);
    }
  }

  @Test
  void helpMustBeUsedAlone() {
    for (String[] args : List.of(new String[]{"--help", "--aeron-directory=unused"},
        new String[]{"--aeron-directory=unused", "--help"}, new String[]{"--help", "--unknown=value"},
        new String[]{"--help", "--help"})) {
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> AeronEngineClient.main(args));
      assertTrue(error.getMessage().contains("--help"), error::getMessage);
      assertTrue(error.getMessage().contains("alone"), error::getMessage);
    }
  }

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
