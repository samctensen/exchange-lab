# Order-book baseline — 24 September 2026

## Scope and environment

- Engine revision: `f0b8c48569fbe8f23082dbd3d41ac700b2807bca` (unchanged engine source).
- Machine: Apple M4 Pro, 14 logical CPUs, 24 GiB RAM, macOS 26.6.2.
- JVM: Homebrew OpenJDK 25.0.4.1, JMH 1.37, G1 GC, 512 MiB initial/maximum heap.
- One worker; two separate JVM forks per case; three one-second warmups and five
  one-second measurements per fork. Nine cases completed, with ten measured iteration
  averages per case. Allocation measured with JMH's GC profiler.
- Normal desktop session, without CPU pinning. No exchange server/client processes
  were found before the run. Other desktop activity was not isolated.
- Validation: 350 application tests and 9 repeated-workload tests passed; both builds'
  formatting checks passed. JMH completed with exit code zero and all depth checks passed.

[Raw JMH results](2026-09-24-book-baseline.json) contain per-fork iteration scores and
profiler data. [Environment metadata](2026-09-24-book-baseline-environment.json) records
the command, source/JAR hashes, and system details.

## Measurements

One operation is a complete cycle, including cancellation or replacement orders.
Refills go through the matching engine. This is not incoming-order latency or an
end-to-end request benchmark. See [workload definitions](../README.md#what-exactly-is-timed).

| Cycle | Resting orders | Mean ± JMH error (µs/cycle) | Allocated B/cycle |
| --- | ---: | ---: | ---: |
| `fourFillsAndRefill` | 1,000 | 17.579 ± 1.739 | 2796.1 |
| `fourFillsAndRefill` | 10,000 | 281.563 ± 17.799 | 2690.0 |
| `fourFillsAndRefill` | 100,000 | 2069.028 ± 66.444 | 2702.5 |
| `passivePlaceAndCancel` | 1,000 | 2.108 ± 0.206 | 296.0 |
| `passivePlaceAndCancel` | 10,000 | 36.199 ± 2.812 | 296.3 |
| `passivePlaceAndCancel` | 100,000 | 268.708 ± 9.469 | 297.9 |
| `singleFillAndRefill` | 1,000 | 6.581 ± 2.460 | 888.0 |
| `singleFillAndRefill` | 10,000 | 74.432 ± 1.383 | 888.5 |
| `singleFillAndRefill` | 100,000 | 540.673 ± 6.148 | 891.8 |

## Conclusion for the next lesson

Increasing depth from 1,000 to 100,000 orders increased the place/cancel cycle from
2.108 to 268.708 µs (about 127×), and the four-fill/refill cycle from 17.579 to
2,069.028 µs (about 118×). This agrees with the source: best-price selection scans
the whole book, and matching repeats that scan for each fill.

The four-fill cycle also processes four replacement asks. That adds four more best-bid
searches, which is why this score must not be compared directly with a sweep-only
measurement that excludes refilling.

The next change should introduce indexed price levels while preserving order-ID
lookup and FIFO priority. Rebuild/install the changed engine and rerun the SAME
harness to compare. No matching-engine optimization is part of this baseline.

## Limits

- These are averages of iteration scores, not individual-order latency percentiles.
- The single-fill cycle at depth 1,000 has relatively wide reported uncertainty
  (6.581 ± 2.460 µs). Do not infer small differences from that row.
- Depth varies while the background price-level count stays fixed. The prepared
  commands and repeated price pattern are deliberate controls, not a traffic model.
- Allocated bytes per cycle are not retained bytes per order. The profiler also sees
  small harness overheads, which can be more visible when fewer cycles run.
- UUID handling, live codecs, gRPC, Aeron, Archive, and disk synchronization are absent.
  These measurements say nothing directly about durable system throughput.
- This is an initial laptop baseline. Keep machine/JVM/harness settings comparable and
  repeat measurements before relying on small performance changes.

## Reproduce

From the repository root, with JDK 25 selected:

```sh
mvn clean install
mvn -f benchmarks/pom.xml clean verify
java -jar benchmarks/target/benchmarks.jar OrderBookBenchmark \
  -prof gc -foe true -rf json -rff benchmarks/target/book-results.json
```

The final path intentionally saves a new result instead of overwriting this baseline.
