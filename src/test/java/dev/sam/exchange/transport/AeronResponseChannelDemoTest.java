package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class AeronResponseChannelDemoTest {
  @Test
  @Timeout(30)
  void eachClientReceivesOnlyItsOwnEchoOnTheSameResponseStream() {
    var result = AeronResponseChannelDemo.run();

    // Swapping the routes or broadcasting both replies must fail these assertions.
    assertAll(() -> assertEquals(List.of("A"), result.clientAReplies()),
        () -> assertEquals(List.of("B"), result.clientBReplies()));
  }
}
