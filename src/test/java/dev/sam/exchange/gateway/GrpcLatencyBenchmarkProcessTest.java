package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(30)
class GrpcLatencyBenchmarkProcessTest {
  @ParameterizedTest
  @ValueSource(ints = {8, 16, 32})
  void measuresTheFullGrpcPathWithConfiguredWindows(int window, @TempDir Path temp) throws Exception {
    Path engineLog = temp.resolve("engine.log");
    Path gatewayLog = temp.resolve("gateway.log");
    Path clientLog = temp.resolve("client.log");
    Process engine = start(temp, engineLog, "dev.sam.exchange.transport.AeronEngineServer",
        temp.resolve("archive").toString(), "--quiet", "--log-window=" + window, "--stage-timing=5,67");
    Process gateway = null;
    Process client = null;
    try {
      awaitOutput(engine, engineLog, "Server ready:");
      gateway = start(temp, gatewayLog, "dev.sam.exchange.gateway.GrpcGatewayServer", "--port=0",
          "--max-in-flight=" + window, "--queue-capacity=128", "--diagnostics");
      awaitOutput(gateway, gatewayLog, "gRPC gateway listening on port ");
      String startup = Files.readString(gatewayLog);
      assertTrue(startup.contains("Gateway max in flight: " + window), startup);
      var port = Pattern.compile("listening on port (\\d+)").matcher(startup);
      assertTrue(port.find(), startup);
      client = start(temp, clientLog, "dev.sam.exchange.gateway.GrpcLatencyBenchmark", "5", "67",
          Integer.toString(window), "5000", port.group(1));
      assertTrue(client.waitFor(10, TimeUnit.SECONDS), "Benchmark did not finish");
      String report = Files.readString(clientLog);
      assertEquals(0, client.exitValue(), report);
      assertTrue(report.contains("Warmup: 5 requests"), report);
      assertTrue(report.contains("Completed: 67 requests"), report);
      assertTrue(report.contains("Successful: 67 requests"), report);
      assertTrue(report.contains("RPC failures: 0"), report);
      assertTrue(report.contains("Successful throughput:"), report);
      assertTrue(report.contains("Success p99:"), report);
      assertTrue(gateway.isAlive() && engine.isAlive());
      gateway.destroy();
      assertTrue(gateway.waitFor(5, TimeUnit.SECONDS), "Gateway did not stop");
      String diagnostics = Files.readString(gatewayLog);
      assertTrue(diagnostics.contains("Gateway accepted: 72"), diagnostics);
      assertTrue(diagnostics.contains("Gateway activated: 72"), diagnostics);
      assertTrue(diagnostics.contains("Gateway queue full: 0"), diagnostics);
      assertTrue(diagnostics.contains("Gateway successful offers: 72"), diagnostics);
      assertTrue(diagnostics.contains("Gateway retries: 0"), diagnostics);
      assertTrue(diagnostics.contains("Gateway reply timeouts: 0"), diagnostics);
      assertFalse(diagnostics.contains("Exception"), diagnostics);
      engine.destroy();
      assertTrue(engine.waitFor(5, TimeUnit.SECONDS), "Engine did not stop");
      String stages = Files.readString(engineLog);
      assertTrue(stages.contains("67/67 samples, skipped 5/5"), stages);
      assertFalse(stages.contains("Exception"), stages);
    } finally {
      stop(client);
      stop(gateway);
      stop(engine);
    }
  }

  private static Process start(Path temp, Path log, String main, String... args) throws Exception {
    List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + temp, "-cp",
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main));
    command.addAll(List.of(args));
    return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
  }

  private static void awaitOutput(Process process, Path log, String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!Files.readString(log).contains(expected)) {
      assertTrue(process.isAlive(), Files.readString(log));
      assertTrue(System.nanoTime() - deadline < 0, Files.readString(log));
      Thread.sleep(10);
    }
  }

  private static void stop(Process process) throws InterruptedException {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      assertTrue(process.waitFor(5, TimeUnit.SECONDS));
    }
  }
}
