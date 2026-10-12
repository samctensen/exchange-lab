package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.aeron.driver.MediaDriver;

class WebSocketGatewayServerTest {
  @TempDir
  Path temporaryDirectory;

  @Test
  void launcherConnectsToTheSelectedAeronDirectory() throws Exception {
    Path driverDirectory = temporaryDirectory.resolve("selected-driver");
    Path output = temporaryDirectory.resolve("gateway.log");
    try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
        .aeronDirectoryName(driverDirectory.toString()).dirDeleteOnShutdown(true))) {
      Process gateway = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
          WebSocketGatewayServer.class.getName(), "--port=0", "--aeron-directory=" + driver.aeronDirectoryName())
          .redirectErrorStream(true).redirectOutput(output.toFile()).start();
      try {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        String log;
        do {
          log = Files.readString(output);
          if (log.contains("WebSocket gateway listening on ws://127.0.0.1:"))
            break;
          Thread.sleep(10);
        } while (gateway.isAlive() && System.nanoTime() - deadline < 0);
        assertTrue(log.contains("WebSocket gateway listening on ws://127.0.0.1:"), log);
      } finally {
        gateway.destroy();
        if (!gateway.waitFor(10, TimeUnit.SECONDS)) {
          gateway.destroyForcibly();
          assertTrue(gateway.waitFor(5, TimeUnit.SECONDS), "Gateway did not stop");
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--aeron-directory=", "--aeron-directory=   ", "--aeron-directory"})
  void invalidAeronDirectoriesIdentifyTheirFlag(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertTrue(error.getMessage().contains("--aeron-directory"));
  }

  @Test
  void helpPrintsUsageWithoutStartingAeron() throws Exception {
    var output = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try (var capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      assertDoesNotThrow(() -> WebSocketGatewayServer.main(new String[]{"--help"}));
    } finally {
      System.setOut(originalOut);
    }
    String usage = output.toString(StandardCharsets.UTF_8);
    assertTrue(usage.contains("WebSocketGatewayServer"));
    assertTrue(usage.contains("--port"));
    assertTrue(usage.contains("--queue-capacity"));
    assertTrue(usage.contains("--max-in-flight"));
    assertTrue(usage.contains("127.0.0.1"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=8080", "--unknown", "--help"})
  void helpMustBeUsedAlone(String otherArgument) {
    for (String[] args : new String[][]{{"--help", otherArgument}, {otherArgument, "--help"}}) {
      var error = assertThrows(IllegalArgumentException.class, () -> WebSocketGatewayServer.main(args));
      assertTrue(error.getMessage().contains("--help must be used alone"));
    }
  }

  @ParameterizedTest
  @CsvSource({"-1,128,8,--port", "65536,128,8,--port", "8080,0,8,--queue-capacity",
      "8080,-1,8,--queue-capacity", "8080,128,0,--max-in-flight", "8080,128,-1,--max-in-flight"})
  void directConfigConstructionRejectsInvalidLimits(int port, int queueCapacity, int maxInFlight, String flag) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> new WebSocketGatewayServer.Config(port, queueCapacity, maxInFlight));
    assertTrue(error.getMessage().contains(flag));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=oops", "--queue-capacity=2147483648", "--max-in-flight="})
  void malformedNumbersIdentifyTheirFlag(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertTrue(error.getMessage().contains(argument.substring(0, argument.indexOf('='))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"--unknown=oops", "--unknown", "--unknown="})
  void unknownFlagsAreReportedBeforeTheirValuesAreParsed(String argument) {
    var error = assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.Config.parse(new String[]{argument}));
    assertEquals("Unknown gateway argument: --unknown", error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"--port=-1", "--port=65536", "--queue-capacity=0", "--queue-capacity=-1", "--max-in-flight=0",
      "--max-in-flight=-1", "--max-in-flight=oops", "--port=", "--host=0.0.0.0", "--unknown=1", "--diagnostics=true",
      "--port"})
  void rejectsInvalidFlagsBeforeConnectingToAeron(String flag) {
    assertThrows(IllegalArgumentException.class, () -> WebSocketGatewayServer.main(new String[]{flag}));
  }

  @Test
  void rejectsDuplicateFlagsBeforeConnectingToAeron() {
    assertThrows(IllegalArgumentException.class,
        () -> WebSocketGatewayServer.main(new String[]{"--port=8080", "--port=8081"}));
  }

  @Test
  void configHasLocalDemoDefaultsAndSupportsExplicitLimits() {
    assertEquals(new WebSocketGatewayServer.Config(8080, 128, 8), WebSocketGatewayServer.Config.parse(new String[0]));
    assertEquals(new WebSocketGatewayServer.Config(0, 16, 4),
        WebSocketGatewayServer.Config.parse(new String[]{"--port=0", "--queue-capacity=16", "--max-in-flight=4"}));
  }
}
