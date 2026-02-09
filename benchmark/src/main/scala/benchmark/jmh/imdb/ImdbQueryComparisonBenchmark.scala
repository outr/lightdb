package benchmark.jmh.imdb

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

class ImdbQueryComparisonBenchmark {
  /**
   * Indexed exact-match filter on titleId.
   * All backends use their respective index structures; this is the parity baseline.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def indexedFilterEquals(state: ImdbQueryComparisonState, bh: Blackhole): Unit = {
    bh.consume(state.indexedFilterEquals())
  }

  /**
   * Substring contains on an indexed title field.
   * Traversal uses n-gram postings to seed candidates (no full scan).
   * SQLite falls back to LIKE '%%' (full table scan).
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def containsQuery(state: ImdbQueryComparisonState, bh: Blackhole): Unit = {
    bh.consume(state.containsQuery())
  }

  /**
   * Prefix startsWith on an indexed title field.
   * Traversal uses persisted prefix postings for efficient candidate generation.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def startsWithQuery(state: ImdbQueryComparisonState, bh: Blackhole): Unit = {
    bh.consume(state.startsWithQuery())
  }

  /**
   * Small-scope In + contains (multi-clause filter).
   * Traversal drives from the In postings stream and verifies contains on the fly.
   * SQLite uses IN(...) + LIKE; Lucene uses a boolean query.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(java.util.concurrent.TimeUnit.SECONDS)
  def scopedContains(state: ImdbQueryComparisonState, bh: Blackhole): Unit = {
    bh.consume(state.scopedContainsQuery())
  }
}
