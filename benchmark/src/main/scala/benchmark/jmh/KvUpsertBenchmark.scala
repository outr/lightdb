package benchmark.jmh

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

class KvUpsertBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def upsert(state: KvState, bh: Blackhole): Unit = {
    bh.consume(state.upsert())
  }
}



