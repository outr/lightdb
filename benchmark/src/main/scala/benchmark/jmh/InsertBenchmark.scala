package benchmark.jmh

import benchmark.jmh.impl.BenchmarkImplementation
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Level, Mode, OperationsPerInvocation, OutputTimeUnit, Param, Scope, Setup, State, TearDown}

import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
class InsertBenchmark {
  @Param(Array(
    "RocksDB", "SQLite",
    "LightDB-RocksDB", "LightDB-HaloDB", "LightDB-H2", "LightDB-LMDB", "LightDB-ChronicleMap", "LightDB-SQLite",
    "LightDB-Lucene"
  ))
  var backend: String = _

  lazy val impl: BenchmarkImplementation = BenchmarkImplementation(backend)
  val iterations = 100_000

  @Setup(Level.Trial)
  def setup(): Unit = impl.init.sync()

  @Benchmark
  @OperationsPerInvocation(100000)
  def insert(): Unit = impl.insert(iterations).sync()

  @TearDown(Level.Trial)
  def teardown(): Unit = impl.dispose.sync()
}

