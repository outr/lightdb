package benchmark.jmh.complete

import lightdb.Sort
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

/** Query throughput against the indexes. Restricted to backends that implement the `Query` DSL
 *  (every Collection + every Split combination) — KV-only backends would just throw
 *  `UnsupportedOperationException` and are filtered out via the `CompleteCollectionState`
 *  `@Param` list.
 *
 *  The four queries cover the most common shapes from
 *  `core/src/test/scala/spec/AbstractBasicSpec.scala`: term equality, range filter, sort +
 *  page, and full-text. Full-text only counts results to keep the comparison fair across
 *  backends with very different doc-materialization costs.
 *
 *  All four use a shared transaction (per-benchmark, per-backend pool) so the measurement
 *  isolates the actual query cost — for splits especially, opening a fresh tx on every
 *  iteration means opening one on the storage side AND the search side, which dominates the
 *  measurement and obscures real per-query differences.
 */
class CompleteQueryBenchmark {

  /** Term equality on an indexed string field. SQL backends use a B-tree index; Lucene/Tantivy
   *  use a postings lookup; KV-backed splits delegate to the search side.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def termFilter(state: CompleteCollectionState, bh: Blackhole): Unit = {
    val coll = state.db.asCollection.get
    val name = state.randomName()
    val n = coll.transaction.shared("CompleteQueryBenchmark.termFilter", 5.seconds) { tx =>
      tx.query.filter(_.name === name).count
    }.sync()
    bh.consume(n)
  }

  /** Numeric range filter. Same access patterns as `termFilter` but on `age`, an indexed Int. */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rangeFilter(state: CompleteCollectionState, bh: Blackhole): Unit = {
    val coll = state.db.asCollection.get
    val (lo, hi) = state.randomAgeRange()
    val n = coll.transaction.shared("CompleteQueryBenchmark.rangeFilter", 5.seconds) { tx =>
      tx.query.filter(_.age BETWEEN (lo, hi)).count
    }.sync()
    bh.consume(n)
  }

  /** Filter + sort + paginate. The "list view" pattern — exercises sort-by-fast-field on
   *  Tantivy/Lucene and ORDER BY + LIMIT on SQL backends.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def filterSortPaginate(state: CompleteCollectionState, bh: Blackhole): Unit = {
    val coll = state.db.asCollection.get
    val (lo, hi) = state.randomAgeRange()
    val first = coll.transaction.shared("CompleteQueryBenchmark.filterSortPaginate", 5.seconds) { tx =>
      tx.query
        .filter(_.age BETWEEN (lo, hi))
        .sort(Sort.ByField(BenchDoc.name))
        .limit(20)
        .toList
    }.sync()
    bh.consume(first)
  }

  /** Full-text query on the `bio` field. Lucene + Tantivy use their analyzers + postings; SQL
   *  backends compile this to a `LIKE` (substring scan) — useful for showing FTS-vs-non-FTS
   *  performance gap on the README chart.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def fullText(state: CompleteCollectionState, bh: Blackhole): Unit = {
    val coll = state.db.asCollection.get
    val term = state.randomBioTerm()
    val n = coll.transaction.shared("CompleteQueryBenchmark.fullText", 5.seconds) { tx =>
      tx.query.filter(_.bio.contains(term)).count
    }.sync()
    bh.consume(n)
  }
}
