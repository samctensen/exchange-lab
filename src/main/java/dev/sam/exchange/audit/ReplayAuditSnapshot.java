package dev.sam.exchange.audit;

import java.math.BigInteger;

/**
 * Immutable totals for a successful prefix of replayed requests. Observed requests partition into
 * unique, duplicate and conflicting requests. Placement, cancellation, rejection and trade totals
 * describe only unique executions. A missing cancellation is not a business rejection. Executed
 * lots and tick-lot notional are exact sums; historySha256 identifies every ordered observation.
 */
public record ReplayAuditSnapshot(long observedRequests, long uniqueRequests, long duplicateRequests,
    long conflictingRequests, long placeRequests, long cancelRequests, long businessRejections, long trades,
    BigInteger executedLots, BigInteger notionalTickLots, String historySha256) {
}
