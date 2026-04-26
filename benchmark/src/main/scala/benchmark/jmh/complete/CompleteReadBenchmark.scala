package benchmark.jmh.complete

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/** Read-side throughput. One trial per `(backend, recordCount)` pair; each benchmark method
 *  measures ops/sec on a hot index.
 */
class CompleteReadBenchmark {

  /** Random `_id` lookup. Universally supported — every backend implements it. */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def get(state: CompleteKvState, bh: Blackhole): Unit = {
    val id = state.randomId
    val doc = state.db.store.transaction(_.get(id)).sync()
    bh.consume(doc)
  }

  /** Full collection scan (single-shot). Heavy operation — tells you how fast each backend can
   *  emit every doc in `recordCount`. Watch out: with `recordCount=100k`, this dominates wall
   *  time vs the throughput-mode benchmarks.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  def fullScan(state: CompleteKvState, bh: Blackhole): Unit = {
    val n = state.db.store.transaction(_.stream.count).sync()
    bh.consume(n)
  }
}
