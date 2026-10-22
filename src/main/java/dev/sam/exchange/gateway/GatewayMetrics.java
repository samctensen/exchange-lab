package dev.sam.exchange.gateway;

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
    return text.toString();
  }

  private static void sample(StringBuilder text, String name, String help, String type, long value) {
    String metric = PREFIX + name;
    text.append("# HELP ").append(metric).append(' ').append(help).append('\n');
    text.append("# TYPE ").append(metric).append(' ').append(type).append('\n');
    text.append(metric).append(' ').append(value).append('\n');
  }
}
