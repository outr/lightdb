package benchmark.jmh

import benchmark.jmh.impl.BenchmarkImplementation
import org.openjdk.jmh.annotations._
import rapid.Task

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
class ReadBenchmark {
  @Param(Array(
    "RocksDB", "SQLite",
    "LightDB-RocksDB", "LightDB-HaloDB", "LightDB-H2",
    "LightDB-LMDB", "LightDB-ChronicleMap", "LightDB-SQLite", "LightDB-Lucene"
  ))
  var backend: String = _

  lazy val impl: BenchmarkImplementation = BenchmarkImplementation(backend)
  val iterations = 100_000

  @Setup(Level.Trial)
  def setup(): Unit = {
    impl.init.sync()
    impl.insert(iterations).sync()
  }

  @Benchmark
  def count(): Unit = {
    val result = impl.count.sync()
    assert(result == iterations, s"Expected $iterations, but got $result")
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def read(): Unit = {
    val result = impl.read.sync()
    assert(result == iterations, s"Expected $iterations reads, but got $result")
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def traverse(): Unit = impl.traverse.sync()

  @TearDown(Level.Trial)
  def teardown(): Unit = impl.dispose.sync()
}