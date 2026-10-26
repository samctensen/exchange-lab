package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonParser;
import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.transport.CommandRequest;

@Timeout(45)
class ArchiveInspectorCliTest {
  @TempDir
  Path directory;

  @Test
  void childProcessPrintsBothFormatsAndNormalRecoveryCanContinueAfterward() throws Exception {
    Path archive = directory.resolve("archive");
    long id;
    long stop;
    try (var runtime = ArchiveRuntime.launch(archive, directory.resolve("writer").toString())) {
      try (var log = ArchiveRequestLog.open(runtime.archive(), ignored -> {
      })) {
        id = log.recordingId();
        ArchiveInspectionTest.append(log, ArchiveInspectionTest.history());
      }
      stop = runtime.archive().getStopPosition(id);
    }
    Execution text = run(directory, "--archive=" + archive, "--levels=1");
    assertEquals(0, text.exit(), text.err());
    assertTrue(text.out().startsWith("Archive inspection v1\n"), text.out());
    assertTrue(text.out().contains("Observed requests: 9\nUnique requests: 7\n"), text.out());
    assertTrue(text.out().contains("  100 1 6\n"), text.out());
    assertFalse(text.out().contains("  98 1 3\n"), "Depth must respect the requested limit");

    Execution json = run(directory, "--archive=" + archive, "--levels=1", "--format=json");
    assertEquals(0, json.exit(), json.err());
    var report = JsonParser.parseString(json.out()).getAsJsonObject();
    assertEquals(1, report.get("formatVersion").getAsInt());
    var requests = report.getAsJsonObject("requests");
    assertEquals("9", requests.get("observed").getAsString());
    assertEquals("7", requests.get("unique").getAsString());
    assertEquals("1", requests.get("duplicates").getAsString());
    assertEquals("1", requests.get("conflicts").getAsString());
    assertEquals("400", report.getAsJsonObject("trades").get("notionalTickLots").getAsString());
    assertEquals("9", report.getAsJsonObject("book").get("bidLots").getAsString());
    assertEquals(1, report.getAsJsonObject("depth").getAsJsonArray("bids").size());
    assertTrue(text.out().contains(report.getAsJsonObject("historyDigest").get("value").getAsString()));

    try (var runtime = ArchiveRuntime.launch(archive, directory.resolve("resumed").toString())) {
      assertEquals(stop, runtime.archive().getStopPosition(id));
      List<CommandRequest> recovered = new ArrayList<>();
      try (var log = ArchiveRequestLog.open(runtime.archive(), recovered::add)) {
        assertEquals(id, log.recordingId());
        assertEquals(ArchiveInspectionTest.history(), recovered);
        ArchiveInspectionTest.append(log, List.of(ArchiveInspectionTest.request(8, new CancelOrder(10))));
      }
      assertEquals(10, ArchiveInspector.inspect(runtime.archive(), 1).audit().observedRequests());
    }
  }

  @Test
  void helpAndInvalidArgumentsDoNotCreateAnArchiveOrRuntime() throws Exception {
    Execution help = run(directory, "--help");
    assertEquals(0, help.exit(), help.err());
    assertTrue(help.out().contains("--archive=path"));
    assertTrue(help.out().contains("Stop the engine"));
    Path absent = directory.resolve("missing");
    for (String[] args : List.of(new String[]{"--archive=" + absent}, new String[]{"--archive=" + absent, "--levels=0"},
        new String[]{"--help", "--archive=" + absent})) {
      Execution failed = run(directory, args);
      assertTrue(failed.exit() != 0, Arrays.toString(args));
      assertEquals("", failed.out());
      assertFalse(Files.exists(absent));
    }
  }

  static Execution run(Path directory, String... args) throws Exception {
    Path output = Files.createTempDirectory(directory, "process-");
    Path temporary = Files.createDirectory(output.resolve("runtime"));
    var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "-Djava.io.tmpdir=" + temporary, "-cp",
        System.getProperty("java.class.path"), ArchiveInspector.class.getName()));
    command.addAll(List.of(args));
    Process process = new ProcessBuilder(command).redirectOutput(output.resolve("stdout").toFile())
        .redirectError(output.resolve("stderr").toFile()).start();
    try {
      assertTrue(process.waitFor(25, TimeUnit.SECONDS), "Inspector process timed out");
      try (var files = Files.list(temporary)) {
        assertEquals(List.of(), files.toList(), "Inspector leaked temporary runtime files");
      }
      return new Execution(process.exitValue(), Files.readString(output.resolve("stdout")),
          Files.readString(output.resolve("stderr")));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Inspector did not terminate");
      }
    }
  }

  record Execution(int exit, String out, String err) {
  }
}
