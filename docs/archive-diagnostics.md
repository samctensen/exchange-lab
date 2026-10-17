# Reliable Archive statistics

`AeronLatencyBenchmark --archive-counters` reads recorder byte and timing counters
from the connected Media Driver. Use a fresh, dedicated engine with one Archive
and no competing recording traffic. Named flags can appear before, between, or
after positional benchmark arguments.

For example, use these engine arguments:

```text
data/counter-run --quiet --aeron-directory=/tmp/exchange-counter-driver
```

Then use these benchmark arguments:

```text
500 2000 8 sleep --archive-counters --aeron-directory=/tmp/exchange-counter-driver
```

Choose a new Archive directory for each run. A reused recording or concurrent
traffic can invalidate the expected byte count.

## Reading the report

The benchmark waits for cumulative counters before warmup, after warmup, and
after measurement. Its fixed cancel workload records one 64-byte Aeron frame per
request. It requires the expected cumulative byte total and 20 ms of unchanged
observations before accepting a boundary snapshot. These waits are outside the
measured latency, throughput, and CPU intervals.

Measured bytes and total write time subtract the post-warmup snapshot from the
final snapshot. Maximum write times are lifetime maxima; their difference is not
the measured interval's maximum. Write time divided by client elapsed time is a
time ratio, not CPU utilization or a latency percentile.

## Invalid and interrupted runs

The reader rejects missing or replaced counters, unexpected extra bytes,
negative values, and a five-second wait timeout. It also rejects a decrease in
bytes, total write time, or maximum write time between observations. Regressing
values cannot establish a trustworthy boundary by merely settling again.

Expected bytes must be nonnegative. Zero is valid for an empty Archive; negative
expectations fail before consulting the clock or counters. Summary inputs must
have nonnegative fields, nondecreasing cumulative values, and a positive measured
duration.

When embedded, interrupting the benchmark's thread aborts both request measurement
and Archive-statistics waits while preserving the interrupt flag. An interrupted
settling boundary cannot return a successful snapshot. Failure in any boundary
wait prevents the benchmark from printing its final performance report. Accepted
requests may still execute; cancellation does not undo recorded commands.

Archive publishes counters separately. They are not an atomic tuple, and the
settling period cannot rule out an arbitrarily delayed update. Increasing fields
may appear at different times; the reader does not assume that maximum and total
timing values form a perfectly synchronized pair. Use the report as supporting
evidence alongside repeated end-to-end measurements.

See the [benchmark instructions](../README.md#measure-ipc-latency-and-throughput)
for startup, idle strategies, concurrency, and measurement limitations.
