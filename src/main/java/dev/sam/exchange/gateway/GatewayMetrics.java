package dev.sam.exchange.gateway;

import java.util.Locale;

/** Read-only Prometheus exposition of one immutable diagnostics snapshot. */
final class GatewayMetrics {
  private static final String PREFIX = "exchange_gateway_";

  private GatewayMetrics() {
  }

  static String render(GatewayDiagnostics.Snapshot snapshot, boolean ready) {
    var text = new StringBuilder();
    sample(text, "accepted_total", "Requests admitted to the gateway queue.", "counter", snapshot.accepted());
    sample(text, "queue_full_total", "Requests rejected because the gateway queue was full.", "counter",
        snapshot.queueFull());
    sample(text, "not_running_total", "Requests rejected because the gateway was not running.", "counter",
        snapshot.notRunning());
    sample(text, "activated_total", "Queued requests activated for processing.", "counter", snapshot.activated());
    sample(text, "completed_total",
        "Requests completed with a definitive engine result, including business rejections.", "counter",
        snapshot.completed());
    sample(text, "business_rejected_total", "Completed requests rejected by the engine's business rules.", "counter",
        snapshot.businessRejected());
    sample(text, "failed_total", "Accepted requests ending in a terminal processing failure.", "counter",
        snapshot.failed());
    sample(text, "unmatched_responses_total", "Engine responses without a matching active request.", "counter",
        snapshot.unmatchedResponses());
    sample(text, "offers_total", "Successful engine publication offers, including retries.", "counter",
        snapshot.offers());
    sample(text, "retries_total", "Successful engine publication offers after the first attempt.", "counter",
        snapshot.retries());
    sample(text, "offer_timeouts_total", "Attempts that timed out before a successful publication offer.", "counter",
        snapshot.offerTimeouts());
    sample(text, "reply_timeouts_total", "Attempts that timed out while awaiting an engine reply.", "counter",
        snapshot.replyTimeouts());
    sample(text, "ready", "Whether the gateway worker is running and its request transport is available.", "gauge",
        ready ? 1 : 0);
    sample(text, "queue_high_water", "Largest observed number of queued requests over the gateway lifetime.", "gauge",
        snapshot.queueHighWater());
    sample(text, "active_high_water", "Largest number of concurrently active requests over the gateway lifetime.",
        "gauge", snapshot.activeHighWater());
    sample(text, "queue_wait_seconds_total", "Total queue wait time of activated requests in seconds.", "counter",
        seconds(snapshot.queueWaitNanos()));
    sample(text, "queue_wait_max_seconds", "Largest queue wait time of activated requests in seconds.", "gauge",
        seconds(snapshot.maxQueueWaitNanos()));
    return text.toString();
  }

  private static void sample(StringBuilder text, String name, String help, String type, long value) {
    sample(text, name, help, type, Long.toString(value));
  }

  private static String seconds(long nanos) {
    return String.format(Locale.ROOT, "%.9f", nanos / 1_000_000_000.0);
  }

  private static void sample(StringBuilder text, String name, String help, String type, String value) {
    String metric = PREFIX + name;
    text.append("# HELP ").append(metric).append(' ').append(help).append('\n');
    text.append("# TYPE ").append(metric).append(' ').append(type).append('\n');
    text.append(metric).append(' ').append(value).append('\n');
  }
}
