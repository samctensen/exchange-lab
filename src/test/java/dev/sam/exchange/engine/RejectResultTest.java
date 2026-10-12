package dev.sam.exchange.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RejectResultTest {
  @Test
  void requiresARejectionReasonBeforeSerialization() {
    assertThrows(NullPointerException.class, () -> new RejectResult(1, null));
  }

  @ParameterizedTest
  @EnumSource(RejectReason.class)
  void preservesEveryReasonAndTheFullWidthOrderId(RejectReason reason) {
    RejectResult result = new RejectResult(Long.MIN_VALUE, reason);
    assertEquals(Long.MIN_VALUE, result.orderId());
    assertEquals(reason, result.reason());
  }
}
