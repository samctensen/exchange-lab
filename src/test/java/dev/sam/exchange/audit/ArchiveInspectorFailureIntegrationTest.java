package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.persistence.ArchiveRequestLog;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;

@Timeout(45)
class ArchiveInspectorFailureIntegrationTest {
  @TempDir
  Path directory;

  @Test
  void malformedFrameAfterValidPrefixFailsWithoutReportOrRecordingChanges() throws Exception {
    Path archive = directory.resolve("archive");
    long id;
    long stop;
    try (var runtime = ArchiveRuntime.launch(archive, directory.resolve("writer").toString());
        ExclusivePublication publication = runtime.archive()
            .addRecordedExclusivePublication("aeron:ipc?alias=exchange-request-log", 2001)) {
      var client = runtime.archive();
      CountersReader counters = client.context().aeron().countersReader();
      ArchiveInspectionTest.await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
          client.archiveId()) != CountersReader.NULL_COUNTER_ID);
      int counter = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), client.archiveId());
      id = RecordingPos.getRecordingId(counters, counter);
      var buffer = new ExpandableArrayBuffer(64);
      int length = new SbeRequestCodec().encode(ArchiveInspectionTest.history().getFirst(), buffer, 0);
      ArchiveInspectionTest.await(() -> publication.offer(buffer, 0, length) >= 0);
      buffer.putInt(0, 0);
      long[] position = {-1};
      ArchiveInspectionTest.await(() -> (position[0] = publication.offer(buffer, 0, 4)) >= 0);
      ArchiveInspectionTest.await(() -> counters.getCounterValue(counter) >= position[0]);
      client.tryStopRecordingByIdentity(id);
      ArchiveInspectionTest.await(() -> client.getStopPosition(id) != AeronArchive.NULL_POSITION);
      stop = client.getStopPosition(id);
    }
    var failed = ArchiveInspectorCliTest.run(directory, "--archive=" + archive, "--format=json");
    assertTrue(failed.exit() != 0);
    assertEquals("", failed.out());
    assertTrue(failed.err().contains("Invalid request in recording " + id), failed.err());
    assertTrue(failed.err().contains("frame end position"), failed.err());
    try (var runtime = ArchiveRuntime.launch(archive, directory.resolve("verify").toString())) {
      assertEquals(stop, runtime.archive().getStopPosition(id));
    }
  }

  @Test
  void refusesActiveOwnerWithoutStoppingItsRecordingOrTakingOverItsDirectory() throws Exception {
    Path archive = directory.resolve("archive");
    try (var runtime = ArchiveRuntime.launch(archive, directory.resolve("owner").toString());
        var log = ArchiveRequestLog.open(runtime.archive(), ignored -> {
        })) {
      ArchiveInspectionTest.append(log, List.of(ArchiveInspectionTest.history().getFirst()));
      assertThrows(IllegalStateException.class, () -> ArchiveInspector.inspect(runtime.archive(), 1));
      var failed = ArchiveInspectorCliTest.run(directory, "--archive=" + archive, "--format=json");
      assertTrue(failed.exit() != 0);
      assertEquals("", failed.out());
      assertTrue(failed.err().toLowerCase(java.util.Locale.ROOT).contains("active"), failed.err());
      // The original owner must remain usable after the competing invocation fails.
      ArchiveInspectionTest.append(log, List.of(ArchiveInspectionTest.request(8, new CancelOrder(10))));
      assertEquals(AeronArchive.NULL_POSITION, runtime.archive().getStopPosition(log.recordingId()));
    }
    assertEquals(2, ArchiveInspector.inspect(archive, 1).audit().observedRequests());
  }
}
