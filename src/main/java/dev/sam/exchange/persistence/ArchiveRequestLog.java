package dev.sam.exchange.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import dev.sam.exchange.protocol.SbeRequestCodec;
import dev.sam.exchange.transport.CommandRequest;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;

/** A single server-owned SBE request recording. Use on one thread; the caller owns the Archive client. */
public final class ArchiveRequestLog implements RequestLog, AutoCloseable {
  private static final String ALIAS = "exchange-request-log";
  private static final int LOG_STREAM_ID = 2001;
  private static final int REPLAY_STREAM_ID = 2002;
  private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

  private record Recording(long id, long start, long stop, int initialTermId, int termLength, int mtu) {
  }

  private final AeronArchive archive;
  private final ExclusivePublication publication;
  private final long subscriptionId;
  private final CountersReader counters;
  private final int counterId;
  private final long recordingId;
  private final SbeRequestCodec codec = new SbeRequestCodec();
  private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
  private long offeredPosition;
  private boolean closed;

  private ArchiveRequestLog(AeronArchive archive, ExclusivePublication publication, long subscriptionId, int counterId,
      long recordingId) {
    this.archive = archive;
    this.publication = publication;
    this.subscriptionId = subscriptionId;
    this.counters = archive.context().aeron().countersReader();
    this.counterId = counterId;
    this.recordingId = recordingId;
    this.offeredPosition = publication.position();
  }

  /** Replay before opening the writer, so no new request can overtake recovery. */
  public static ArchiveRequestLog open(AeronArchive archive, Consumer<CommandRequest> recovery) {
    Recording recording = findRecording(archive);
    if (recording != null)
      replay(archive, recording, recovery);

    ChannelUriStringBuilder channel = new ChannelUriStringBuilder().media("ipc").alias(ALIAS);
    if (recording != null) {
      channel.initialPosition(recording.stop(), recording.initialTermId(), recording.termLength()).mtu(recording.mtu());
    }
    ExclusivePublication publication = archive.context().aeron().addExclusivePublication(channel.build(),
        LOG_STREAM_ID);
    long subscriptionId = -1;
    try {
      // Bind recording to our publication's session, excluding every other publisher on this stream.
      String recordingChannel = ChannelUri.addSessionId(channel.build(), publication.sessionId());
      subscriptionId = recording == null
          ? archive.startRecording(recordingChannel, LOG_STREAM_ID, SourceLocation.LOCAL)
          : archive.extendRecording(recording.id(), recordingChannel, LOG_STREAM_ID, SourceLocation.LOCAL);
      CountersReader counters = archive.context().aeron().countersReader();
      await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(),
          archive.archiveId()) != CountersReader.NULL_COUNTER_ID, archive, "request recording to start");
      int counter = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
      long id = RecordingPos.getRecordingId(counters, counter);
      if (recording != null && id != recording.id()) {
        throw new IllegalStateException("Archive extended the wrong recording");
      }
      return new ArchiveRequestLog(archive, publication, subscriptionId, counter, id);
    } catch (RuntimeException | Error failure) {
      publication.close();
      if (subscriptionId >= 0) {
        try {
          archive.tryStopRecording(subscriptionId);
        } catch (RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      throw failure;
    }
  }

  public long recordingId() {
    return recordingId;
  }

  @Override
  public long offer(CommandRequest request) {
    ensureActive();
    int length = codec.encode(request, buffer, 0);
    long result = publication.offer(buffer, 0, length);
    if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
      throw new IllegalStateException("Request log publication failed; offer result: " + result);
    }
    if (result >= 0)
      offeredPosition = result;
    return result;
  }

  @Override
  public boolean isRecorded(long position) {
    ensureActive();
    return counters.getCounterValue(counterId) >= position;
  }

  private void ensureActive() {
    if (closed)
      throw new IllegalStateException("Request log is closed");
    archive.checkForErrorResponse();
    if (!RecordingPos.isActive(counters, counterId, recordingId)) {
      throw new IllegalStateException("Request recording stopped unexpectedly: " + recordingId);
    }
  }

  /** Stream recovery without materializing the recording as a list. */
  public static void replay(AeronArchive archive, Consumer<CommandRequest> recovery) {
    Recording recording = findRecording(archive);
    if (recording != null)
      replay(archive, recording, recovery);
  }

  private static Recording findRecording(AeronArchive archive) {
    List<Recording> found = new ArrayList<>();
    archive.listRecordingsForUri(0, 2, "alias=" + ALIAS, LOG_STREAM_ID,
        (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
            initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
            originalChannel, sourceIdentity) -> found.add(
                new Recording(recordingId, startPosition, stopPosition, initialTermId, termBufferLength, mtuLength)));
    if (found.size() > 1)
      throw new IllegalStateException("Multiple request recordings; cannot determine one ordered log");
    if (found.isEmpty())
      return null;
    Recording recording = found.getFirst();
    if (recording.start() != 0 || recording.stop() == AeronArchive.NULL_POSITION) {
      throw new IllegalStateException("Recovery requires a complete, stopped request recording: " + recording.id());
    }
    return recording;
  }

  private static void replay(AeronArchive archive, Recording recording, Consumer<CommandRequest> recovery) {
    if (recording.start() == recording.stop())
      return;
    SbeRequestCodec decoder = new SbeRequestCodec();
    try (Subscription replay = archive.replay(recording.id(), recording.start(), recording.stop() - recording.start(),
        "aeron:ipc", REPLAY_STREAM_ID)) {
      await(() -> replay.imageCount() > 0, archive, "request replay to connect");
      Image image = replay.imageAtIndex(0);
      FragmentAssembler assembler = new FragmentAssembler(
          (buffer, offset, length, header) -> recovery.accept(decoder.decode(buffer, offset, length)));
      SleepingIdleStrategy idle = new SleepingIdleStrategy();
      long deadline = System.nanoTime() + TIMEOUT_NS;
      while (image.position() < recording.stop()) {
        int fragments = image.poll(assembler, 10);
        if (image.position() >= recording.stop())
          break;
        if (image.isClosed())
          throw new IllegalStateException("Request replay closed before its saved stop position");
        // Bound inactivity, not total recovery time: a large healthy recording may take longer than five seconds.
        if (fragments > 0)
          deadline = System.nanoTime() + TIMEOUT_NS;
        checkWait(archive, deadline, "request replay progress");
        idle.idle(fragments);
      }
    }
  }

  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    try {
      // An offered request may be recorded even if shutdown happened before application or reply.
      // Recovery then applies it; the caller must retry uncertain outcomes using the same UUID.
      await(() -> RecordingPos.isActive(counters, counterId, recordingId)
          ? counters.getCounterValue(counterId) >= offeredPosition
          : archive.getStopPosition(recordingId) >= offeredPosition, archive, "final request recording");
      archive.tryStopRecording(subscriptionId);
      await(() -> archive.getStopPosition(recordingId) >= offeredPosition, archive, "request recording to stop");
    } finally {
      publication.close();
    }
  }

  private static void await(BooleanSupplier done, AeronArchive archive, String operation) {
    SleepingIdleStrategy idle = new SleepingIdleStrategy();
    long deadline = System.nanoTime() + TIMEOUT_NS;
    while (!done.getAsBoolean()) {
      checkWait(archive, deadline, operation);
      idle.idle();
    }
  }

  private static void checkWait(AeronArchive archive, long deadline, String operation) {
    archive.checkForErrorResponse();
    if (Thread.currentThread().isInterrupted())
      throw new IllegalStateException("Interrupted waiting for " + operation);
    if (System.nanoTime() - deadline >= 0)
      throw new IllegalStateException("Timed out waiting for " + operation);
  }
}
