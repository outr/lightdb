package benchmark.jmh.imdb

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

class ImdbGetByIdBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def getById(state: ImdbState, bh: Blackhole): Unit = {
    bh.consume(state.randomGetAka())
  }
}



