package benchmark.jmh

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

class KvGetBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def get(state: KvState, bh: Blackhole): Unit = {
    bh.consume(state.get())
  }
}



