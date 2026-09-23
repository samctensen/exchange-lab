package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.ReplayResult;
import dev.sam.exchange.engine.ReplayRunner;
import dev.sam.exchange.engine.Side;
import dev.sam.exchange.protocol.SbeRequestCodec;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;

// A standalone lesson: record commands, shut down, then rebuild a book from the saved recording.
public class AeronArchiveDemo {
  private static final String CHANNEL = "aeron:ipc";
  // Archive uses stream 10 for control requests and 20 for responses by default.
  private static final int COMMAND_STREAM_ID = 1001;
  private static final int REPLAY_STREAM_ID = 1002;
  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

  public static void main(String[] args) throws IOException {
    Path directory = Files.createTempDirectory("exchange-archive-");
    List<CommandRequest> requests = List.of(
        new CommandRequest(UUID.randomUUID(), new PlaceOrder(1L, Side.BID, 100L, 10L)),
        new CommandRequest(UUID.randomUUID(), new PlaceOrder(2L, Side.ASK, 99L, 4L)));

    long recordingId = record(directory, requests);
    System.out.println("Recording " + recordingId + " saved; Archive and driver stopped.");

    List<CommandRequest> replayed = replay(directory, recordingId);
    System.out.println("Requests preserved: " + requests.equals(replayed));

    ReplayResult recovered = new ReplayRunner().replay(replayed.stream().map(CommandRequest::command).toList());

    recovered.results().forEach(System.out::println);
    System.out.println("Recovered book: " + recovered.bookSnapshot());
  }

  static long record(Path directory, List<CommandRequest> requests) {
    try (ArchivingMediaDriver driver = launchArchive(directory); AeronArchive archive = connect(driver)) {
      // This ID identifies the recording subscription, not the saved recording itself.
      long recordingSubscriptionId = archive.startRecording(CHANNEL, COMMAND_STREAM_ID, SourceLocation.LOCAL);
      try (ExclusivePublication publication = archive.context().aeron().addExclusivePublication(CHANNEL,
          COMMAND_STREAM_ID)) {
        CountersReader counters = archive.context().aeron().countersReader();
        // Each publication session gets its own recording. Wait for Archive to expose its progress counter.
        await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
            archive.archiveId()) != CountersReader.NULL_COUNTER_ID, archive, "recording to start");
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
        long recordingId = RecordingPos.getRecordingId(counters, counterId);

        SbeRequestCodec codec = new SbeRequestCodec();
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);

        for (CommandRequest request : requests) {
          int length = codec.encode(request, buffer, 0);
          offer(archive, publication, buffer, length);
        }

        // offer() only queues bytes. Wait until Archive has written through the final byte position.
        // A position counts framed stream bytes; it is not a command count.
        long stopPosition = publication.position();
        await(() -> {
          if (!RecordingPos.isActive(counters, counterId, recordingId)) {
            throw new IllegalStateException("Recording stopped before all commands were recorded");
          }
          return counters.getCounterValue(counterId) >= stopPosition;
        }, archive, "Archive to catch up");

        archive.stopRecording(recordingSubscriptionId);
        await(() -> archive.getStopPosition(recordingId) == stopPosition, archive, "recording to stop");
        return recordingId;
      }
    }
  }

  static List<CommandRequest> replay(Path directory, long recordingId) {
    List<CommandRequest> requests = new ArrayList<>();
    SbeRequestCodec codec = new SbeRequestCodec();

    try (ArchivingMediaDriver driver = launchArchive(directory); AeronArchive archive = connect(driver)) {
      long startPosition = archive.getStartPosition(recordingId);
      long stopPosition = archive.getStopPosition(recordingId);
      if (stopPosition == AeronArchive.NULL_POSITION) {
        throw new IllegalStateException("Replay requires a stopped recording");
      }
      if (stopPosition == startPosition) {
        return List.of();
      }

      // Archive publishes the stored bytes on a separate stream. replay() creates a subscription
      // filtered to this replay's session, so another replay cannot mix its messages into ours.
      try (Subscription replay = archive.replay(recordingId, startPosition, stopPosition - startPosition, CHANNEL,
          REPLAY_STREAM_ID)) {
        await(() -> replay.imageCount() > 0, archive, "replay to connect");
        Image image = replay.imageAtIndex(0);
        FragmentAssembler assembler = new FragmentAssembler(
            (buffer, offset, length, header) -> requests.add(codec.decode(buffer, offset, length)));

        // Retain the image so we can inspect its position even after the replay disconnects.
        // Completion uses the saved stop position, without needing the original command list.
        SleepingIdleStrategy idle = new SleepingIdleStrategy();
        long deadline = System.nanoTime() + TIMEOUT_NS;
        while (image.position() < stopPosition) {
          int fragments = image.poll(assembler, 10);
          if (image.position() >= stopPosition) {
            break;
          }
          if (image.isClosed()) {
            throw new IllegalStateException("Replay closed before reaching the recorded stop position");
          }
          checkWait(archive, deadline, "replay to finish");
          idle.idle(fragments);
        }
      }
    }
    return List.copyOf(requests);
  }

  private static ArchivingMediaDriver launchArchive(Path directory) {
    // Driver files are disposable shared memory; the archive directory holds the persistent recording.
    // A unique driver directory keeps this lesson independent of a running exchange server.
    return ArchivingMediaDriver.launch(
        new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName()).dirDeleteOnShutdown(true),
        new Archive.Context().archiveDir(directory.toFile()).deleteArchiveOnStart(false).controlChannelEnabled(false)
            .localControlChannel(CHANNEL).recordingEventsEnabled(false)
            // Required Archive configuration; this demo does not use remote replication.
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(CHANNEL))
            // Normal-restart recovery only: writes reach the OS cache, with no explicit disk force.
            .fileSyncLevel(0).catalogFileSyncLevel(0));
  }

  private static AeronArchive connect(ArchivingMediaDriver driver) {
    // AeronArchive is the control client (start/stop/replay). It creates and owns an Aeron connection.
    return AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(driver.mediaDriver().aeronDirectoryName())
        .controlRequestChannel(CHANNEL).controlRequestStreamId(driver.archive().context().localControlStreamId())
        .controlResponseChannel(CHANNEL).messageTimeoutNs(TIMEOUT_NS));
  }

  private static void offer(AeronArchive archive, Publication publication, ExpandableArrayBuffer buffer, int length) {
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    long deadline = System.nanoTime() + TIMEOUT_NS;
    long result;
    while ((result = publication.offer(buffer, 0, length)) < 0) {
      if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
        throw new IllegalStateException("Publication cannot send; offer result: " + result);
      }
      checkWait(archive, deadline, "command offer; last result: " + result);
      idle.idle();
    }
  }

  private static void await(BooleanSupplier ready, AeronArchive archive, String operation) {
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    long deadline = System.nanoTime() + TIMEOUT_NS;
    while (!ready.getAsBoolean()) {
      checkWait(archive, deadline, operation);
      idle.idle();
    }
  }

  private static void checkWait(AeronArchive archive, long deadline, String operation) {
    archive.checkForErrorResponse();
    if (Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("Interrupted waiting for " + operation);
    }
    if (System.nanoTime() - deadline >= 0) {
      throw new IllegalStateException("Timed out waiting for " + operation);
    }
  }
}
