package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GatewayMetricsTest {
  private static final GatewayDiagnostics.Snapshot SNAPSHOT = new GatewayDiagnostics.Snapshot(101, 7, 9, 85, 11, 13, 17,
      98, 1_250_000_001L, 750_000_001L, 19, 23, 107, 29, 31, 37);

  private static final String ADMISSION = """
      # HELP exchange_gateway_accepted_total Requests admitted to the gateway queue.
      # TYPE exchange_gateway_accepted_total counter
      exchange_gateway_accepted_total 101
      # HELP exchange_gateway_queue_full_total Requests rejected because the gateway queue was full.
      # TYPE exchange_gateway_queue_full_total counter
      exchange_gateway_queue_full_total 7
      # HELP exchange_gateway_not_running_total Requests rejected because the gateway was not running.
      # TYPE exchange_gateway_not_running_total counter
      exchange_gateway_not_running_total 9
      # HELP exchange_gateway_activated_total Queued requests activated for processing.
      # TYPE exchange_gateway_activated_total counter
      exchange_gateway_activated_total 98
      """;

  @Test
  void exposesAdmissionCountsWithPrometheusMetadata() {
    assertEquals(ADMISSION, GatewayMetrics.render(SNAPSHOT, true));
  }
}
