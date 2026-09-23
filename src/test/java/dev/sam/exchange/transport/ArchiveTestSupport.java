package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.concurrent.SleepingIdleStrategy;

import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;

final class ArchiveTestSupport {
  static List<CommandRequest> readAll(Path directory) throws IOException {
    List<CommandRequest> requests = new ArrayList<>();
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory, CommonContext.generateRandomDirName())) {
      ArchiveRequestLog.replay(runtime.archive(), requests::add);
    }
    return List.copyOf(requests);
  }

  static void record(Path directory, List<CommandRequest> requests) throws IOException {
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(directory, CommonContext.generateRandomDirName());
        ArchiveRequestLog log = ArchiveRequestLog.open(runtime.archive(), r -> {
        })) {
      for (CommandRequest request : requests) {
        long[] position = {-1};
        await(() -> (position[0] = log.offer(request)) >= 0);
        await(() -> log.isRecorded(position[0]));
      }
    }
  }

  static AeronArchive connect(Path driverDirectory) {
    return AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(driverDirectory.toString())
        .controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc"));
  }

  static long recordingId(AeronArchive archive) {
    long[] id = {-1};
    int count = archive.listRecordingsForUri(0, 2, "alias=exchange-request-log", 2001,
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) -> id[0] = c);
    assertTrue(count == 1, "Expected exactly one request recording");
    return id[0];
  }

  static void await(BooleanSupplier done) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    while (!done.getAsBoolean()) {
      assertTrue(System.nanoTime() - deadline < 0, "Timed out waiting for Archive");
      idle.idle();
    }
  }
}
