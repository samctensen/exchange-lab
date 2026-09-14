# Exchange Lab

This is my hands-on exploration of modern Java backend programming and high-frequency trading architecture, starting with the very basics.

I'm building a small exchange engine to refresh my Java fundamentals and understand how an order book works before moving into services, performance tuning, and distributed systems. The longer-term direction is a deterministic engine running with Aeron and Aeron Cluster.

## Where it is today

The project currently uses plain Java 25, Maven, and JUnit 5:

- Order validation with prices in integer ticks and quantities in lots.
- An in-memory order book with add, find, cancel, and best bid/ask selection.
- Arrival-order priority between orders at the same price.
- Remaining-quantity tracking, partial fills, and removal of fully filled orders.
- One trade per matching call, executed at the resting order's price.

This is an early learning implementation. Best-price selection currently scans the book; performance targets, concurrency, networking, persistence, and clustering are still ahead.

## Run locally

Install JDK 25 and Maven 3.9+, and make sure `mvn -v` reports Java 25.

```sh
mvn test             # Run the tests
mvn verify           # Build, test, and check formatting
mvn spotless:apply   # Format Java sources
```

The tests are the main way to exercise the engine right now. `App.java` is a scratch entry point from the initial validation exercises, not a running exchange service.

Engine code lives in `src/main/java/dev/sam/exchange/engine`, with corresponding tests under `src/test/java/dev/sam/exchange/engine`. Zed and Spotless share Eclipse formatting preferences; Zed is configured to format Java when saving.

## What I want to explore next

1. Complete the matching loop and strengthen tests for order-book behavior.
2. Expose the engine through a service and learn about command and event boundaries.
3. Measure throughput and tail latency, then study allocation, GC, data layout, and JVM behavior.
4. Introduce Aeron messaging, followed by Aeron Cluster, deterministic replay, snapshots, and failover.

The idea is to build understanding incrementally and let measurements guide the performance work.
