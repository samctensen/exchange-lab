package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class GatewayDiagnosticsTest {
  @Test
  void capturedReportRemainsStableAfterFurtherActivity() {
    var diagnostics = new GatewayDiagnostics();
    diagnostics.accepted(2);
    diagnostics.accepted(1);
    diagnostics.queueFull();
    diagnostics.activated(2_000, 1);
    diagnostics.activated(6_000, 2);
    diagnostics.offered(false);
    diagnostics.offered(true);
    diagnostics.timedOut(false);
    diagnostics.timedOut(true);

    var snapshot = diagnostics.snapshot();
    String report = snapshot.summarize();
    diagnostics.accepted(4);
    diagnostics.activated(30_000, 3);
    diagnostics.offered(true);
    diagnostics.timedOut(true);

    assertEquals(2, snapshot.accepted());
    assertEquals(1, snapshot.queueFull());
    assertEquals(2, snapshot.activated());
    assertEquals(8_000, snapshot.queueWaitNanos());
    assertEquals(6_000, snapshot.maxQueueWaitNanos());
    assertEquals(2, snapshot.queueHighWater());
    assertEquals(2, snapshot.activeHighWater());
    assertEquals(2, snapshot.offers());
    assertEquals(1, snapshot.retries());
    assertEquals(1, snapshot.offerTimeouts());
    assertEquals(1, snapshot.replyTimeouts());
    assertEquals(report, snapshot.summarize());
    assertTrue(report.contains("Gateway queue wait mean: 4.000 us"), report);
    assertTrue(report.contains("Gateway queue wait max: 6.000 us"), report);
    assertTrue(diagnostics.summarize().contains("Gateway accepted: 3"));
  }

  @Test
  void emptySnapshotProducesFiniteQueueWaitMean() {
    String report = new GatewayDiagnostics().snapshot().summarize();
    assertTrue(report.contains("Gateway queue wait mean: 0.000 us"), report);
  }
}
