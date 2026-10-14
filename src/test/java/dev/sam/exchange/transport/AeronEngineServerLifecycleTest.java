package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AeronEngineServerLifecycleTest {
  @ParameterizedTest
  @ValueSource(strings = {"", "archive"})
  @Timeout(10)
  void startupFailureDoesNotLeaveHookWaitingForTheCaller(String archiveChild, @TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("driver");
    Path archiveDirectory = driverDirectory.resolve(archiveChild);
    Path output = tempDir.resolve("startup.log");
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process caller = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
        classpath, ExitAfterFailedLaunch.class.getName(), archiveDirectory.toString(),
        "--aeron-directory=" + driverDirectory).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      boolean exited = caller.waitFor(3, TimeUnit.SECONDS);
      String log = Files.readString(output);
      assertTrue(log.contains("Expected startup failure caught"), log);
      assertTrue(exited, "Shutdown hook waited for the caller after server startup failed\n" + log);
      assertEquals(0, caller.exitValue(), log);
      assertFalse(log.contains("Server shutdown exceeded"), log);
      assertFalse(Files.exists(driverDirectory));
    } finally {
      if (caller.isAlive()) {
        caller.destroyForcibly();
        caller.waitFor(3, TimeUnit.SECONDS);
      }
    }
  }

  public static final class ExitAfterFailedLaunch {
    public static void main(String[] args) throws Exception {
      try {
        AeronEngineServer.main(args);
        throw new AssertionError("Expected unsafe archive directory to fail server startup");
      } catch (IllegalArgumentException error) {
        if (!error.getMessage().contains("Archive directory must be outside")) {
          throw error;
        }
        System.out.println("Expected startup failure caught");
      }
      // This caller stays alive inside System.exit while the JVM runs any remaining hooks.
      System.exit(0);
    }
  }
}
