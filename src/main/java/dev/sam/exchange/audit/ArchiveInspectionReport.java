package dev.sam.exchange.audit;

import java.util.List;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import dev.sam.exchange.engine.PriceLevelSnapshot;

/** Formats aggregate inspection results without exposing individual request or order identities. */
final class ArchiveInspectionReport {
  private ArchiveInspectionReport() {
  }

  static String text(ArchiveInspection inspection) {
    var a = inspection.audit();
    var b = inspection.book();
    var text = new StringBuilder("Archive inspection v1\n");
    line(text, "History SHA-256 (sha256-chain-sbe-v1)", a.historySha256());
    line(text, "Observed requests", a.observedRequests());
    line(text, "Unique requests", a.uniqueRequests());
    line(text, "Duplicate requests", a.duplicateRequests());
    line(text, "Conflicting requests", a.conflictingRequests());
    line(text, "Place requests", a.placeRequests());
    line(text, "Cancel requests", a.cancelRequests());
    line(text, "Business rejections", a.businessRejections());
    line(text, "Trades", a.trades());
    line(text, "Executed lots", a.executedLots());
    line(text, "Notional tick-lots", a.notionalTickLots());
    line(text, "Book bid orders", b.bidOrders());
    line(text, "Book ask orders", b.askOrders());
    line(text, "Book bid levels", b.bidLevels());
    line(text, "Book ask levels", b.askLevels());
    line(text, "Book bid lots", b.bidLots());
    line(text, "Book ask lots", b.askLots());
    levels(text, "Bid", inspection.depth().bids());
    levels(text, "Ask", inspection.depth().asks());
    return text.toString();
  }

  static String json(ArchiveInspection inspection) {
    var a = inspection.audit();
    var b = inspection.book();
    var root = new JsonObject();
    root.addProperty("formatVersion", 1);
    var digest = new JsonObject();
    digest.addProperty("algorithm", "sha256-chain-sbe-v1");
    digest.addProperty("value", a.historySha256());
    root.add("historyDigest", digest);
    var requests = new JsonObject();
    requests.addProperty("observed", Long.toString(a.observedRequests()));
    requests.addProperty("unique", Long.toString(a.uniqueRequests()));
    requests.addProperty("duplicates", Long.toString(a.duplicateRequests()));
    requests.addProperty("conflicts", Long.toString(a.conflictingRequests()));
    requests.addProperty("placements", Long.toString(a.placeRequests()));
    requests.addProperty("cancellations", Long.toString(a.cancelRequests()));
    requests.addProperty("businessRejections", Long.toString(a.businessRejections()));
    root.add("requests", requests);
    var trades = new JsonObject();
    trades.addProperty("count", Long.toString(a.trades()));
    trades.addProperty("quantityLots", a.executedLots().toString());
    trades.addProperty("notionalTickLots", a.notionalTickLots().toString());
    root.add("trades", trades);
    var book = new JsonObject();
    book.addProperty("bidOrders", b.bidOrders());
    book.addProperty("askOrders", b.askOrders());
    book.addProperty("bidLevels", b.bidLevels());
    book.addProperty("askLevels", b.askLevels());
    book.addProperty("bidLots", b.bidLots().toString());
    book.addProperty("askLots", b.askLots().toString());
    root.add("book", book);
    var depth = new JsonObject();
    depth.add("bids", jsonLevels(inspection.depth().bids()));
    depth.add("asks", jsonLevels(inspection.depth().asks()));
    root.add("depth", depth);
    return root + "\n";
  }

  private static JsonArray jsonLevels(List<PriceLevelSnapshot> levels) {
    var array = new JsonArray();
    for (var level : levels) {
      var entry = new JsonObject();
      entry.addProperty("priceTicks", Long.toString(level.priceTicks()));
      entry.addProperty("orderCount", level.orderCount());
      entry.addProperty("quantityLots", level.quantityLots().toString());
      array.add(entry);
    }
    return array;
  }

  private static void line(StringBuilder text, String name, Object value) {
    text.append(name).append(": ").append(value).append('\n');
  }

  private static void levels(StringBuilder text, String side, List<PriceLevelSnapshot> levels) {
    text.append(side).append(" depth (priceTicks orders quantityLots):\n");
    if (levels.isEmpty())
      text.append("  (empty)\n");
    for (var level : levels)
      text.append("  ").append(level.priceTicks()).append(' ').append(level.orderCount()).append(' ')
          .append(level.quantityLots()).append('\n');
  }
}
