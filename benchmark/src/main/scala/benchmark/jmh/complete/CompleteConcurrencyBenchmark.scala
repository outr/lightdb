package benchmark.jmh.complete

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadLocalRandom

/** Concurrent mixed read/write workload. Each `@Benchmark` is annotated with a fixed thread
 *  count so JMH spawns N worker threads for that iteration. The three classes are otherwise
 *  identical; running all of them gives the per-backend scaling curve.
 *
 *  Workload mix: 80% point reads, 20% upserts. Mirrors a typical "live KV with light writes"
 *  pattern — heavy writes saturate every backend differently and obscure the read-side
 *  contention curve.
 */
class CompleteConcurrency1Benchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(1)
  def mixed(state: CompleteKvState, bh: Blackhole): Unit =
    ConcurrencyOps.runMixed(state, bh)
}

class CompleteConcurrency4Benchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(4)
  def mixed(state: CompleteKvState, bh: Blackhole): Unit =
    ConcurrencyOps.runMixed(state, bh)
}

class CompleteConcurrency16Benchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(16)
  def mixed(state: CompleteKvState, bh: Blackhole): Unit =
    ConcurrencyOps.runMixed(state, bh)
}

private object ConcurrencyOps {
  /** 80% reads, 20% writes. Threading control comes from JMH `@Threads`. */
  def runMixed(state: CompleteKvState, bh: Blackhole): Unit = {
    val r = ThreadLocalRandom.current().nextInt(100)
    if (r < 80) {
      val doc = state.db.store.transaction(_.get(state.randomId)).sync()
      bh.consume(doc)
    } else {
      val doc = state.nextDoc()
      state.db.store.transaction(_.upsert(doc).unit).sync()
      bh.consume(doc)
    }
  }
}
