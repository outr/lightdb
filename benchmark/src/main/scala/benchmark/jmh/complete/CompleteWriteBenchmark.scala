package benchmark.jmh.complete

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/** Write workloads.
 *
 *  - `upsert`: random-key upsert against the existing doc set, one doc per invocation
 *    (Throughput mode). The "live update" pattern.
 *  - `bulkInsert`: single-shot, inserts `recordCount` docs into an empty backend in one
 *    transaction with the chosen `BatchConfig`. The "ingest" pattern. Uses its own state
 *    (`CompleteWriteState`) so the bulk doesn't contaminate the upsert workload's starting
 *    size.
 */
class CompleteWriteBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def upsert(state: CompleteKvState, bh: Blackhole): Unit = {
    val doc = state.nextDoc()
    state.db.store.transaction(_.upsert(doc).unit).sync()
    bh.consume(doc)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1)
  def bulkInsert(state: CompleteWriteState, bh: Blackhole): Unit = {
    state.runBulkInsert()
    bh.consume(state.lastInsertCount)
  }
}
