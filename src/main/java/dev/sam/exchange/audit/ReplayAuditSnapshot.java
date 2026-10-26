package dev.sam.exchange.audit;

import java.math.BigInteger;

/** Totals for a completed prefix of replayed requests. Execution totals exclude cached responses. */
public record ReplayAuditSnapshot(long observedRequests, long uniqueRequests, long duplicateRequests,
    long conflictingRequests, long placeRequests, long cancelRequests, long businessRejections, long trades,
    BigInteger executedLots, BigInteger notionalTickLots, String historySha256) {
}
