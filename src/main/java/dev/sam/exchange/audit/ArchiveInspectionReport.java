package dev.sam.exchange.audit;

import java.util.List;

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
