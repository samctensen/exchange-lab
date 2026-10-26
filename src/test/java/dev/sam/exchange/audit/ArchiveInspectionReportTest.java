package dev.sam.exchange.audit;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import com.google.gson.JsonParser;

import dev.sam.exchange.engine.BookDepth;
import dev.sam.exchange.engine.BookSummary;
import dev.sam.exchange.engine.PriceLevelSnapshot;

class ArchiveInspectionReportTest {
  static final String HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

  static ArchiveInspection fixture() {
    return new ArchiveInspection(
        new ReplayAuditSnapshot(10, 7, 2, 1, 5, 2, 1, 3, new BigInteger("18446744073709551614"),
            new BigInteger("170141183460469231694793815568465002498"), HASH),
        new BookSummary(3, 4, 2, 2, new BigInteger("18446744073709551615"), BigInteger.valueOf(40)),
        new BookDepth(List.of(new PriceLevelSnapshot(100, 2, new BigInteger("18446744073709551614"))),
            List.of(new PriceLevelSnapshot(101, 3, BigInteger.valueOf(30)))));
  }

  @Test
  void textPreservesExactTotalsAndSeparatesWholeBookFromDisplayedDepth() {
    String report = ArchiveInspectionReport.text(fixture());
    assertEquals("""
        Archive inspection v1
        History SHA-256 (sha256-chain-sbe-v1): %s
        Observed requests: 10
        Unique requests: 7
        Duplicate requests: 2
        Conflicting requests: 1
        Place requests: 5
        Cancel requests: 2
        Business rejections: 1
        Trades: 3
        Executed lots: 18446744073709551614
        Notional tick-lots: 170141183460469231694793815568465002498
        Book bid orders: 3
        Book ask orders: 4
        Book bid levels: 2
        Book ask levels: 2
        Book bid lots: 18446744073709551615
        Book ask lots: 40
        Bid depth (priceTicks orders quantityLots):
          100 2 18446744073709551614
        Ask depth (priceTicks orders quantityLots):
          101 3 30
        """.formatted(HASH), report);
  }

  @Test
  void emptyDepthIsExplicitAndTextDoesNotDependOnTheDefaultLocale() {
    var empty = new ArchiveInspection(fixture().audit(), new BookSummary(0, 0, 0, 0, BigInteger.ZERO, BigInteger.ZERO),
        new BookDepth(List.of(), List.of()));
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.GERMANY);
      String report = ArchiveInspectionReport.text(empty);
      assertTrue(report.contains("Bid depth (priceTicks orders quantityLots):\n  (empty)\n"));
      assertTrue(report.endsWith("Ask depth (priceTicks orders quantityLots):\n  (empty)\n"));
      assertTrue(report.contains("Executed lots: 18446744073709551614\n"));
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  void inspectionRejectsMissingComponents() {
    var f = fixture();
    assertThrows(NullPointerException.class, () -> new ArchiveInspection(null, f.book(), f.depth()));
    assertThrows(NullPointerException.class, () -> new ArchiveInspection(f.audit(), null, f.depth()));
    assertThrows(NullPointerException.class, () -> new ArchiveInspection(f.audit(), f.book(), null));
  }

  @Test
  void jsonUsesAVersionedShapeAndExactDecimalStringsForWideNumbers() {
    var f = fixture();
    var largePrice = new ArchiveInspection(f.audit(), f.book(), new BookDepth(
        List.of(new PriceLevelSnapshot(Long.MAX_VALUE, 2, new BigInteger("18446744073709551614"))), List.of()));
    String report = ArchiveInspectionReport.json(largePrice);
    assertEquals(JsonParser.parseString("""
        {"formatVersion":1,"historyDigest":{"algorithm":"sha256-chain-sbe-v1","value":"%s"},
        "requests":{"observed":"10","unique":"7","duplicates":"2","conflicts":"1",
          "placements":"5","cancellations":"2","businessRejections":"1"},
        "trades":{"count":"3","quantityLots":"18446744073709551614",
          "notionalTickLots":"170141183460469231694793815568465002498"},
        "book":{"bidOrders":3,"askOrders":4,"bidLevels":2,"askLevels":2,
          "bidLots":"18446744073709551615","askLots":"40"},
        "depth":{"bids":[{"priceTicks":"9223372036854775807","orderCount":2,
          "quantityLots":"18446744073709551614"}],"asks":[]}}
        """.formatted(HASH)), JsonParser.parseString(report));
    assertTrue(report.endsWith("\n"));
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.GERMANY);
      assertEquals(report, ArchiveInspectionReport.json(largePrice));
    } finally {
      Locale.setDefault(original);
    }
  }
}
