package dev.sam.exchange.gateway;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

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

  private static final String OUTCOMES = """
      # HELP exchange_gateway_completed_total Requests completed with a definitive engine result, including business rejections.
      # TYPE exchange_gateway_completed_total counter
      exchange_gateway_completed_total 85
      # HELP exchange_gateway_business_rejected_total Completed requests rejected by the engine's business rules.
      # TYPE exchange_gateway_business_rejected_total counter
      exchange_gateway_business_rejected_total 11
      # HELP exchange_gateway_failed_total Accepted requests ending in a terminal processing failure.
      # TYPE exchange_gateway_failed_total counter
      exchange_gateway_failed_total 13
      # HELP exchange_gateway_unmatched_responses_total Engine responses without a matching active request.
      # TYPE exchange_gateway_unmatched_responses_total counter
      exchange_gateway_unmatched_responses_total 17
      """;

  private static final String TRANSPORT = """
      # HELP exchange_gateway_offers_total Successful engine publication offers, including retries.
      # TYPE exchange_gateway_offers_total counter
      exchange_gateway_offers_total 107
      # HELP exchange_gateway_retries_total Successful engine publication offers after the first attempt.
      # TYPE exchange_gateway_retries_total counter
      exchange_gateway_retries_total 29
      # HELP exchange_gateway_offer_timeouts_total Attempts that timed out before a successful publication offer.
      # TYPE exchange_gateway_offer_timeouts_total counter
      exchange_gateway_offer_timeouts_total 31
      # HELP exchange_gateway_reply_timeouts_total Attempts that timed out while awaiting an engine reply.
      # TYPE exchange_gateway_reply_timeouts_total counter
      exchange_gateway_reply_timeouts_total 37
      """;

  private static final String CAPACITY = """
      # HELP exchange_gateway_ready Whether the gateway worker is running and its request transport is available.
      # TYPE exchange_gateway_ready gauge
      exchange_gateway_ready 1
      # HELP exchange_gateway_queue_high_water Largest observed number of queued requests over the gateway lifetime.
      # TYPE exchange_gateway_queue_high_water gauge
      exchange_gateway_queue_high_water 19
      # HELP exchange_gateway_active_high_water Largest number of concurrently active requests over the gateway lifetime.
      # TYPE exchange_gateway_active_high_water gauge
      exchange_gateway_active_high_water 23
      # HELP exchange_gateway_queue_wait_seconds_total Total queue wait time of activated requests in seconds.
      # TYPE exchange_gateway_queue_wait_seconds_total counter
      exchange_gateway_queue_wait_seconds_total 1.250000001
      # HELP exchange_gateway_queue_wait_max_seconds Largest queue wait time of activated requests in seconds.
      # TYPE exchange_gateway_queue_wait_max_seconds gauge
      exchange_gateway_queue_wait_max_seconds 0.750000001
      """;

  @Test
  void emitsOnlyStableAggregateMetricsWithNanosecondsConvertedToSeconds() {
    assertEquals(ADMISSION + OUTCOMES + TRANSPORT + CAPACITY, GatewayMetrics.render(SNAPSHOT, true));
  }

  @Test
  @ResourceLock(Resources.LOCALE)
  void decimalSecondsRemainValidUnderNonEnglishLocale() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.GERMANY);
      assertEquals(ADMISSION + OUTCOMES + TRANSPORT + CAPACITY, GatewayMetrics.render(SNAPSHOT, true));
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void unavailableEmptyGatewayHasFiniteZeroDurations() {
    String rendered = GatewayMetrics.render(new GatewayDiagnostics().snapshot(), false);
    assertTrue(rendered.endsWith(
        """
            # HELP exchange_gateway_ready Whether the gateway worker is running and its request transport is available.
            # TYPE exchange_gateway_ready gauge
            exchange_gateway_ready 0
            # HELP exchange_gateway_queue_high_water Largest observed number of queued requests over the gateway lifetime.
            # TYPE exchange_gateway_queue_high_water gauge
            exchange_gateway_queue_high_water 0
            # HELP exchange_gateway_active_high_water Largest number of concurrently active requests over the gateway lifetime.
            # TYPE exchange_gateway_active_high_water gauge
            exchange_gateway_active_high_water 0
            # HELP exchange_gateway_queue_wait_seconds_total Total queue wait time of activated requests in seconds.
            # TYPE exchange_gateway_queue_wait_seconds_total counter
            exchange_gateway_queue_wait_seconds_total 0.000000000
            # HELP exchange_gateway_queue_wait_max_seconds Largest queue wait time of activated requests in seconds.
            # TYPE exchange_gateway_queue_wait_max_seconds gauge
            exchange_gateway_queue_wait_max_seconds 0.000000000
            """));
  }

  @Test
  void distinguishesOffersRetriesAndEachTimeoutStage() {
    assertTrue(GatewayMetrics.render(SNAPSHOT, true).contains(TRANSPORT));
  }

  @Test
  void exposesTerminalOutcomesWithoutConflatingBusinessRejectionsWithFailures() {
    assertTrue(GatewayMetrics.render(SNAPSHOT, true).contains(OUTCOMES));
  }

  @Test
  void exposesAdmissionCountsWithPrometheusMetadata() {
    assertTrue(GatewayMetrics.render(SNAPSHOT, true).startsWith(ADMISSION));
  }
}
