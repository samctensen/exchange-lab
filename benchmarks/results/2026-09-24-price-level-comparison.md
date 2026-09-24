# Price-level comparison — 24 September 2026

## Conclusion

Price-level indexing removed the strong dependence on total resting-order count in
these workloads. Across 1,000–100,000 resting orders, the new implementation measured
about 0.043–0.045 µs per place/cancel cycle, 0.170–0.183 µs per single-fill/refill
cycle, and 0.439–0.475 µs per four-fill/refill cycle.

At 100,000 orders, the four-fill/refill cycle dropped from 2,069.028 µs to 0.439 µs.
The large ratio comes from eliminating repeated scans of 100,000 orders while this
fixture keeps the number of price levels fixed. It is a result for this controlled
workload, not a prediction of production request latency or throughput.

Allocation increased in all three workloads. That is a separate cost to investigate
if profiling shows GC pressure in a representative workload; it does not erase the
measured improvement in search cost.

## What changed

The old `bestBid()` and `bestAsk()` walked every resting order. The new book maintains:

- A descending price map for bids and an ascending price map for asks.
- A FIFO `LinkedHashMap` inside each price level.
- The existing order-ID map, pointing to the same `RestingOrder` objects as the levels.
- Shared removal logic that updates both indexes and deletes empty levels.

Finding the best level now follows the price tree, then takes the first order at
that price. Tree navigation depends on the number of levels, rather than scanning
the total order count. This is not a claim of constant-time tree lookup. The workload
holds the level count fixed as it increases depth, so nearly flat timings are expected.

## Timing

One operation is a **complete cycle**, including cancellation or replacement orders.
The single-fill cycle processes two commands; the four-fill cycle processes five.
These scores are not the latency of one incoming order. See the
[workload definitions](../README.md#what-exactly-is-timed).

Values are mean ± JMH-reported error in microseconds per cycle. Ratios use the
unrounded means; they are descriptive comparisons, not confidence bounds.

| Cycle | Resting orders | Scan-based (µs/cycle) | Price levels (µs/cycle) | Old/new mean ratio |
| --- | ---: | ---: | ---: | ---: |
| Place/cancel | 1,000 | 2.108 ± 0.206 | 0.045 ± 0.001 | 46.3× |
| Place/cancel | 10,000 | 36.199 ± 2.812 | 0.045 ± 0.001 | 799.3× |
| Place/cancel | 100,000 | 268.708 ± 9.469 | 0.043 ± 0.004 | 6,312.3× |
| Single fill/refill | 1,000 | 6.581 ± 2.460 | 0.179 ± 0.025 | 36.8× |
| Single fill/refill | 10,000 | 74.432 ± 1.383 | 0.170 ± 0.034 | 438.6× |
| Single fill/refill | 100,000 | 540.673 ± 6.148 | 0.183 ± 0.006 | 2,955.3× |
| Four fills/refill | 1,000 | 17.579 ± 1.739 | 0.463 ± 0.007 | 37.9× |
| Four fills/refill | 10,000 | 281.563 ± 17.799 | 0.475 ± 0.062 | 593.4× |
| Four fills/refill | 100,000 | 2069.028 ± 66.444 | 0.439 ± 0.004 | 4,711.6× |

Small differences among the new implementation's depths do not establish that a
larger book is faster. The useful finding is that the previous depth-dependent
increase disappeared in this experiment.

## Allocation tradeoff

JMH's `gc.alloc.rate.norm` measures allocated bytes per cycle, including engine
results and temporary objects. It does not measure retained memory per resting order.

| Cycle | Resting orders | Scan-based B/cycle | Price levels B/cycle |
| --- | ---: | ---: | ---: |
| Place/cancel | 1,000 | 296.0 | 416.0 |
| Place/cancel | 10,000 | 296.3 | 416.0 |
| Place/cancel | 100,000 | 297.9 | 416.0 |
| Single fill/refill | 1,000 | 888.0 | 1624.0 |
| Single fill/refill | 10,000 | 888.5 | 1596.0 |
| Single fill/refill | 100,000 | 891.8 | 1624.0 |
| Four fills/refill | 1,000 | 2796.1 | 4440.0 |
| Four fills/refill | 10,000 | 2690.0 | 4436.0 |
| Four fills/refill | 100,000 | 2702.5 | 4360.0 |

The source suggests several contributors: each order has membership in a second
map; incoming crossing orders are still inserted before matching; exhausted levels
and their maps are recreated by refills. These are plausible explanations, not an
allocation-by-class profile. No further optimization is included in this comparison.

## Validation and reproducibility

- Baseline engine: `f0b8c48569fbe8f23082dbd3d41ac700b2807bca`.
- Compared engine: that base plus the uncommitted `OrderBook` and `PriceLevel`
  changes saved in [the engine patch](2026-09-24-price-level-engine.patch).
- **370 application tests passed**, with zero failures, errors, or skips. This
  includes 14 `PriceLevel` cases and six new integration cases covering both sides:
  partial/full fills, cancellation within a level, advancing to the next price,
  removing/recreating levels, and reusing IDs without retaining old FIFO priority.
- **Nine benchmark workload tests passed**, checking trades and restored snapshots
  across repeated cycles. Both builds' formatting checks passed.
- All nine JMH cases completed successfully. Each contains two forks with five
  measured iterations per fork. The benchmark source hash matches the baseline.
- JVM version, arguments, timing settings, thread count, parameters, measurement
  mode, and units were compared with the baseline JSON and matched.
- The benchmark JAR's engine classes match the verified engine JAR. Sources in the
  temporary build copy match the working tree after measurement.

The first application build in the editor-watched checkout encountered eight gRPC
test errors from a compiled class containing JDT "unresolved compilation" stubs.
Validation therefore used a temporary source copy outside the watched project.
The full clean build passed there without gRPC source changes. The affected class
in the active checkout subsequently showed its expected generated superclass again.

Both measurements used an Apple M4 Pro, 14 logical CPUs, 24 GiB RAM, macOS 26.6.2,
Homebrew OpenJDK 25.0.4.1, and JMH 1.37. Each case used one worker, two JVM forks,
three one-second warmups, five one-second measurements, a 512 MiB heap, and G1 GC.
`JAVA_TOOL_OPTIONS` and `JDK_JAVA_OPTIONS` were unset for measurement, matching the
baseline. No exchange server/client processes were found before or after the new run.
Builds and tests completed before measurement started.

Run from the repository root with JDK 25 selected; use a separate source copy if
the editor is also writing Maven's output directory:

```sh
mvn clean install
mvn -f benchmarks/pom.xml clean verify
env -u JAVA_TOOL_OPTIONS -u JDK_JAVA_OPTIONS \
  java -jar benchmarks/target/benchmarks.jar OrderBookBenchmark \
  -prof gc -foe true -rf json -rff benchmarks/target/book-results.json
```

Reinstall the engine and rebuild the benchmark after every engine change. A stale
locally installed engine JAR would invalidate the comparison.

Artifacts: [baseline report](2026-09-24-book-baseline.md),
[baseline JSON](2026-09-24-book-baseline.json),
[new JSON](2026-09-24-price-level.json), and
[new environment and verification metadata](2026-09-24-price-level-environment.json).

## Limits and lesson

This is a normal desktop session without CPU pinning or isolation from desktop
activity. Both runs use prepared commands, reused IDs, repeated prices, and a small
fixed set of price levels. They do not establish performance with many distinct
levels, random traffic, partial fills, or concurrent clients. These are iteration
averages, not individual-request p99 latency measurements.

UUID deduplication, codecs, gRPC, Aeron, Archive, disk synchronization, and queueing
are outside the measured path. A faster matcher does not imply the same speedup for
durable requests. That boundary needs its own end-to-end benchmark.

The lesson is that indexing removed unnecessary work: finding the next matching
order no longer requires inspecting unrelated orders throughout the book. Correctness
tests preserve FIFO priority and index consistency while the benchmark checks the
intended scaling improvement. The price-level refactor is ready for the next lesson;
allocation tuning can follow evidence from more representative profiling.
