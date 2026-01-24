package benchmark.jmh.imdb

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

class ImdbFindByTitleIdBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def findByTitleId(state: ImdbState, bh: Blackhole): Unit = {
    bh.consume(state.randomFindByTitleId())
  }
}



