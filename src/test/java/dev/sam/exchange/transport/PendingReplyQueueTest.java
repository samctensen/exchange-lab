package dev.sam.exchange.transport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.sam.exchange.engine.CancelResult;

class PendingReplyQueueTest {
  @ParameterizedTest
  @CsvSource({"0, 100", "-1, 100", "2, 0", "2, -1"})
  void rejectsNonpositiveLimits(int maxReplies, long maxBytes) {
    assertThrows(IllegalArgumentException.class, () -> new PendingReplyQueue(maxReplies, maxBytes));
  }

  @Test
  void emptyReadsReturnNullWithoutChangingCapacity() {
    PendingReplyQueue queue = new PendingReplyQueue(2, 100);

    assertNull(assertDoesNotThrow(queue::peek));
    assertNull(queue.poll());
    assertNull(queue.poll());
    assertEquals(0, queue.size());
    assertEquals(0L, queue.queuedBytes());
    assertTrue(queue.offer(reply(1), 100));
  }

  @Test
  void peekRetainsTheHeadAndPollReturnsRepliesInOrder() {
    PendingReplyQueue queue = new PendingReplyQueue(3, 100);
    PendingReply first = reply(1);
    PendingReply second = reply(2);
    PendingReply third = reply(3);
    assertTrue(queue.offer(first, 20));
    assertTrue(queue.offer(second, 30));
    assertTrue(queue.offer(third, 40));

    assertEquals(first, queue.peek());
    assertEquals(first, queue.peek());
    assertEquals(3, queue.size());
    assertEquals(90L, queue.queuedBytes());
    assertEquals(first, queue.poll());
    assertEquals(70L, queue.queuedBytes());
    assertEquals(second, queue.peek());
    assertEquals(second, queue.poll());
    assertEquals(40L, queue.queuedBytes());
    assertEquals(third, queue.poll());
    assertEquals(0, queue.size());
    assertEquals(0L, queue.queuedBytes());
    assertNull(queue.poll());
  }

  @Test
  void countLimitRejectsWithoutOverwritingAndPollFreesASlot() {
    PendingReplyQueue queue = new PendingReplyQueue(2, 100);
    PendingReply first = reply(1);
    PendingReply second = reply(2);
    PendingReply third = reply(3);
    assertTrue(queue.offer(first, 10));
    assertTrue(queue.offer(second, 20));

    assertFalse(queue.offer(third, 30));
    assertEquals(2, queue.size());
    assertEquals(30L, queue.queuedBytes());
    assertEquals(first, queue.poll());
    assertTrue(queue.offer(third, 30));
    assertEquals(50L, queue.queuedBytes());
    assertEquals(second, queue.poll());
    assertEquals(third, queue.poll());
    assertEquals(0L, queue.queuedBytes());
  }

  @Test
  void byteLimitAcceptsAnExactFitAndPollReleasesOnlyTheRemovedBytes() {
    PendingReplyQueue queue = new PendingReplyQueue(3, 100);
    PendingReply first = reply(1);
    PendingReply second = reply(2);
    PendingReply third = reply(3);
    assertTrue(queue.offer(first, 40));
    assertTrue(queue.offer(second, 60));

    assertFalse(queue.offer(third, 1));
    assertEquals(2, queue.size());
    assertEquals(100L, queue.queuedBytes());
    assertEquals(first, queue.poll());
    assertEquals(60L, queue.queuedBytes());
    assertFalse(queue.offer(third, 41));
    assertEquals(1, queue.size());
    assertEquals(60L, queue.queuedBytes());
    assertTrue(queue.offer(third, 40));
    assertEquals(100L, queue.queuedBytes());
    assertEquals(second, queue.poll());
    assertEquals(third, queue.poll());
  }

  @Test
  void aSingleOversizedReplyDoesNotConsumeCapacity() {
    PendingReplyQueue queue = new PendingReplyQueue(1, 100);
    assertFalse(queue.offer(reply(1), 101));
    assertEquals(0, queue.size());
    assertEquals(0L, queue.queuedBytes());
    assertTrue(queue.offer(reply(2), 100));
    assertEquals(reply(2), queue.poll());
  }

  @Test
  void clearDiscardsOldRepliesAndRestoresTheWholeBudget() {
    PendingReplyQueue queue = new PendingReplyQueue(2, 100);
    assertTrue(queue.offer(reply(1), 40));
    assertTrue(queue.offer(reply(2), 60));

    queue.clear();
    queue.clear();
    assertEquals(0, queue.size());
    assertEquals(0L, queue.queuedBytes());
    assertNull(queue.poll());
    assertTrue(queue.offer(reply(3), 100));
    assertEquals(reply(3), queue.poll());
    assertEquals(0L, queue.queuedBytes());
  }

  @ParameterizedTest
  @CsvSource({"0, false", "-1, false", "0, true", "-1, true"})
  void rejectsNonpositiveLengthsWithoutChangingTheQueue(int length, boolean full) {
    PendingReplyQueue queue = new PendingReplyQueue(full ? 1 : 2, 100);
    PendingReply first = reply(1);
    assertTrue(queue.offer(first, 40));

    assertThrows(IllegalArgumentException.class, () -> queue.offer(reply(2), length));
    assertEquals(1, queue.size());
    assertEquals(40L, queue.queuedBytes());
    assertEquals(first, queue.poll());
    assertEquals(0L, queue.queuedBytes());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsNullRepliesEvenWhenFull(boolean full) {
    PendingReplyQueue queue = new PendingReplyQueue(full ? 1 : 2, 100);
    PendingReply first = reply(1);
    assertTrue(queue.offer(first, 40));

    assertThrows(NullPointerException.class, () -> queue.offer(null, 10));
    assertEquals(1, queue.size());
    assertEquals(40L, queue.queuedBytes());
    assertEquals(first, queue.poll());
    assertEquals(0L, queue.queuedBytes());
  }

  @Test
  void byteAccountingSupportsTotalsLargerThanAnInt() {
    PendingReplyQueue queue = new PendingReplyQueue(3, 2_147_483_654L);
    assertTrue(queue.offer(reply(1), Integer.MAX_VALUE));
    assertTrue(queue.offer(reply(2), 7));
    assertEquals(2_147_483_654L, queue.queuedBytes());
    assertFalse(queue.offer(reply(3), 1));

    assertEquals(reply(1), queue.poll());
    assertEquals(7L, queue.queuedBytes());
    assertTrue(queue.offer(reply(3), Integer.MAX_VALUE));
    assertEquals(2_147_483_654L, queue.queuedBytes());
  }

  private static PendingReply reply(long id) {
    return new PendingReply(new CommandResponse(new UUID(0, id), new CancelResult(id, false)), 500, null, 100, 200);
  }
}
