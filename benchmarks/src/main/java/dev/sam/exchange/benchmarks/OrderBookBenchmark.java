package dev.sam.exchange.benchmarks;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import dev.sam.exchange.engine.CancelOrder;
import dev.sam.exchange.engine.MatchingEngine;
import dev.sam.exchange.engine.OrderBook;
import dev.sam.exchange.engine.PlaceOrder;
import dev.sam.exchange.engine.PlaceResult;
import dev.sam.exchange.engine.Side;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
public class OrderBookBenchmark {
  @Param({"1000", "10000", "100000"})
  public int depth;

  OrderBook book;
  private MatchingEngine engine;
  private PlaceOrder passive;
  private CancelOrder cancelPassive;
  private PlaceOrder singleFill;
  private PlaceOrder fourFills;
  private PlaceOrder[] refills;

  @Setup(Level.Trial)
  public void setup() {
    if (depth < 4) {
      throw new IllegalArgumentException("depth must allow at least four resting asks");
    }
    book = new OrderBook();
    engine = new MatchingEngine(book);

    // Seed directly once: setup cost is outside measurement. Both sides remain uncrossed.
    // More depth adds orders to the same background price levels.
    for (int i = 0; i < depth - 4; i++) {
      Side side = i % 2 == 0 ? Side.BID : Side.ASK;
      long price = side == Side.BID ? 90L - i % 20 : 110L + i % 20;
      book.add(new PlaceOrder(i + 1L, side, price, 10L));
    }
    refills = new PlaceOrder[4];
    for (int i = 0; i < refills.length; i++) {
      refills[i] = new PlaceOrder(depth - 3L + i, Side.ASK, 100L + i, 1L);
      book.add(refills[i]);
    }

    // Commands are prepared outside timing. Reuse an ID only after its order has left the book.
    long incomingId = depth + 1L;
    passive = new PlaceOrder(incomingId, Side.BID, 80L, 1L);
    cancelPassive = new CancelOrder(incomingId);
    singleFill = new PlaceOrder(incomingId, Side.BID, 100L, 1L);
    fourFills = new PlaceOrder(incomingId, Side.BID, 103L, 4L);
  }

  @Benchmark
  public PlaceResult passivePlaceAndCancel() {
    PlaceResult result = (PlaceResult) engine.process(passive);
    engine.process(cancelPassive);
    return result;
  }

  @Benchmark
  public PlaceResult singleFillAndRefill() {
    PlaceResult result = (PlaceResult) engine.process(singleFill);
    engine.process(refills[0]);
    return result;
  }

  @Benchmark
  public PlaceResult fourFillsAndRefill() {
    PlaceResult result = (PlaceResult) engine.process(fourFills);
    for (PlaceOrder refill : refills) {
      engine.process(refill);
    }
    return result;
  }

  @TearDown(Level.Iteration)
  public void verifyDepth() {
    // Outside the timed loop: fail a run if a broken cycle starts measuring a smaller book.
    if (book.size() != depth || book.find(depth + 1L).isPresent()) {
      throw new IllegalStateException("Benchmark cycle did not restore the resting book");
    }
  }
}
