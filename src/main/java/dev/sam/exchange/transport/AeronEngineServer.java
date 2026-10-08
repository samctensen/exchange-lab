package dev.sam.exchange.transport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;

import io.aeron.Aeron;
import io.aeron.Subscription;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.persistence.ArchiveRuntime;
import dev.sam.exchange.persistence.ArchiveRequestLog;

public class AeronEngineServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length == 1 && "--help".equals(args[0])) {
      System.out.print(EngineServerConfig.help());
      return;
    }
    EngineServerConfig config = EngineServerConfig.parse(args);
    EngineStageTimings stageTimings = config.stageSamples() == 0
        ? null
        : new EngineStageTimings(config.stageWarmup(), config.stageSamples());
    ReplyDeliveryStats replyStats = config.replyStats() ? new ReplyDeliveryStats() : null;

    AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    AtomicReference<Throwable> agentFailure = new AtomicReference<>();
    ErrorHandler errorHandler = error -> {
      agentFailure.compareAndSet(null, error);
      shutdownRequested.set(true);
      Thread.currentThread().interrupt();
    };
    Thread serverThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Ask the main thread to begin closing the server.
      shutdownRequested.set(true);
      try {
        // Give it time to finish and close the try-with-resources block.
        serverThread.join(10_000);
        if (serverThread.isAlive()) {
          System.err.println("Server shutdown exceeded 10 seconds");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "server-shutdown"));

    Path archiveDirectory = config.archiveDirectory();
    RequestStateMachine stateMachine = new RequestStateMachine(new MatchingEngine(new OrderBook()));

    // Both processes use this directory to connect to the same media driver.
    String aeronDirectory = Path.of(System.getProperty("java.io.tmpdir"), "exchange-lab-aeron").toString();

    // The server owns the driver; resources close in reverse order on exit.
    try (ArchiveRuntime runtime = ArchiveRuntime.launch(archiveDirectory, aeronDirectory);
        // Replay the saved log before creating the live request subscription, then extend that same recording.
        ArchiveRequestLog requestLog = ArchiveRequestLog.open(runtime.archive(), stateMachine::process);
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
        Subscription subscription = aeron.addSubscription("aeron:ipc", 1);
        ResponsePublicationRegistry responsePublications = new ResponsePublicationRegistry(aeron);
        // Busy spinning keeps polling for requests; budget a dedicated core for this runner.
        AgentRunner runner = new AgentRunner(new BusySpinIdleStrategy(), errorHandler, null,
            new AeronEngineAgent(subscription, responsePublications, stateMachine, requestLog, !config.quiet(),
                SystemNanoClock.INSTANCE, stageTimings, config.logWindow(), config.replyDelivery(), replyStats))) {

      AgentRunner.startOnThread(runner);

      System.out.println("Server ready: " + aeronDirectory);
      System.out.println("Archive: " + archiveDirectory);
      System.out.println("Request recording: " + requestLog.recordingId());
      System.out.println("Engine log window: " + config.logWindow());
      System.out.println("Reply policy: capacity=" + config.replyDelivery().maxReplies() + ", bytes="
          + config.replyDelivery().maxBytes() + ", timeout-ms=" + config.replyDelivery().timeout().toMillis());
      System.out.println("Waiting for requests.");

      // The runner processes requests. Main waits here to keep resources open.
      while (!shutdownRequested.get() && !runner.isClosed()) {
        Thread.sleep(10);
      }
    } finally {
      // Resource closure has stopped the writer before main reads its samples.
      if (stageTimings != null) {
        System.out.print(stageTimings.summarize());
      }
      if (replyStats != null) {
        System.out.print(replyStats.summarize());
      }
    }
    Throwable failure = agentFailure.get();
    if (failure != null) {
      throw new IllegalStateException("Engine agent failed", failure);
    }
  }
}
