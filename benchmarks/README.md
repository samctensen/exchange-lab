# Order-book benchmarks

Measure how `MatchingEngine.process()` scales with resting book depth. The saved
baseline captures the original scan-based book; the same harness measures the
price-level implementation for comparison.

This is a standalone Maven project using [OpenJDK JMH](https://github.com/openjdk/jmh).
It depends on the locally installed exchange JAR. JMH and its generated harness are
kept out of the application's dependencies and normal build.

## Build and run

Use JDK 25 for both Maven and `java` (`mvn -v` and `java -version` should agree).
From the repository root:

```sh
# Repeat this after EVERY engine change, so benchmarks use the new engine JAR.
mvn clean install
mvn -f benchmarks/pom.xml clean verify

# Run all three workloads at all three depths, including allocation profiling.
# Run from a terminal, without a debugger or other load generators.
java -jar benchmarks/target/benchmarks.jar OrderBookBenchmark \
  -prof gc -foe true -rf json -rff benchmarks/target/book-results.json
```

A full run takes roughly three minutes with the checked-in settings. `-foe true`
ends the run if a benchmark fails. Do not run the builds while measuring performance.

To check wiring quickly (this is NOT a comparison-quality measurement):

```sh
java -jar benchmarks/target/benchmarks.jar OrderBookBenchmark \
  -p depth=1000 -f 1 -wi 1 -i 1 -w 200ms -r 200ms -foe true
```

The benchmark project's `verify` runs its workload tests and formatting check.
The root build runs the application tests; it does not discover this separate project.
To format benchmark Java: `mvn -f benchmarks/pom.xml spotless:apply`.

## What exactly is timed?

**One JMH operation is one complete cycle**, with restoration included:

| Benchmark | Commands timed in each cycle | Expected trades |
| --- | --- | --- |
| `passivePlaceAndCancel` | Place one non-crossing bid, then cancel it | None |
| `singleFillAndRefill` | Place a bid that fills one ask, then replace that ask | One at 100 ticks |
| `fourFillsAndRefill` | Place a bid that fills four asks, then replace all four | Four at 100, 101, 102, 103 ticks |

All refills go through `MatchingEngine.process()`, just like the incoming commands.
Refill time is therefore part of the score. Do not label a cycle's score as the
latency of its incoming order, or compare it directly with a benchmark that excludes refills.

The initial total book depth is 1,000, 10,000, or 100,000 resting orders. It contains
four one-lot asks at 100–103 and background orders on both sides: bids below 100 and
asks above 103. Background orders have ten lots each. Increasing depth adds orders
at the same background price levels; it does not increase the number of levels.
Each cycle restores the same orders and quantities. Refills at distinct prices can
move in global insertion order without changing priority at any one price.

The initial book and command objects are prepared outside the timed loop. Commands
reuse IDs only after the previous order is removed. This measures the pure matching
engine; it does not invoke UUID deduplication, codecs, gRPC, Aeron, or Archive.

## Reading the harness

Read `src/main/java/dev/sam/exchange/benchmarks/OrderBookBenchmark.java`:

- `@Param`: JMH runs each method at every configured depth.
- `@State(Scope.Thread)`: each benchmark worker owns its book and engine. One worker
  is used, matching our single-writer design.
- `@Setup(Level.Trial)`: builds the initial fixture once, before warmup and measurement.
- `@Warmup`: runs three one-second iterations before collecting scores.
- `@Measurement`: collects five one-second iterations per fork.
- `@Fork`: repeats in two fresh JVM processes, each with a 512 MiB heap and G1 GC.
- Returning the result makes it available to JMH, helping prevent unused work from
  being optimized away.
- `@TearDown(Level.Iteration)`: checks book depth outside the timed loop. The separate
  JUnit tests check exact trades and the full restored book across repeated cycles.

We include restoration in the measured cycle instead of resetting the whole book
before every invocation. JMH's [per-invocation setup sample](https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_38_PerInvokeSetup.java)
explains how setup and timing overhead can distort a small benchmark.

## Reading the output

- `ns/op`: average nanoseconds per **cycle**. Divide by 1,000 for microseconds.
- `Score ± Error`: JMH's average and its reported uncertainty, not p50/p99 latency.
- `gc.alloc.rate.norm`: allocated bytes per cycle, not retained heap per resting order.
- `gc.count` and `gc.time`: garbage collection activity during measurement.

Keep the JSON: it records JVM settings, parameters, individual iteration scores,
reported uncertainty, and profiler measurements. A successful run is not a speed
assertion; there is no CI threshold tied to this laptop's timings.

## What this baseline can and cannot tell us

It can reveal depth-dependent cost in the original scan-based book and support a
before/after comparison of price-level indexing. Compare the SAME workload, depth,
JDK, heap, GC, machine, and harness settings. If results are close or noisy, rerun
before claiming a small improvement.

This is a deliberately repeatable workload with prepared commands and a small
fixed set of prices. It does not model random client traffic, increasing numbers of
price levels, partial fills, cancellation-only load, or end-to-end queueing. Those
are separate experiments; all matching behavior still needs its correctness tests.
Disk durability is outside this benchmark, so these results cannot predict durable
requests per second. Use the existing IPC benchmark for that separate boundary.

See [the first saved baseline](results/2026-09-24-book-baseline.md) and
[the price-level comparison](results/2026-09-24-price-level-comparison.md).

## Price-level refactor

The original book scanned all resting orders to find the best opposite order, and
repeated that scan for each fill. The new book uses sorted price maps, FIFO order
within each price, and a separate order-ID index pointing to the same resting objects.
Cancellation and full fills remove orders from both indexes and delete empty levels.
The benchmark workloads and settings are unchanged between the saved runs.
