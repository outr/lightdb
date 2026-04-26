package benchmark.jmh.complete

import lightdb.id.Id
import org.openjdk.jmh.annotations.*

import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

/** Shared lifecycle code for the JMH @State classes. We can't make `var backend: String` an
 *  abstract member and override it (Scala 3 won't let you re-declare a `var`), so each state
 *  class duplicates its own `@Param` declarations and delegates the heavy lifting here.
 */
private[complete] object BenchSetup {
  def init(spec: BackendCatalog.Spec, recordCount: Int, preLoad: Boolean): (BenchDb, Path, Array[Id[BenchDoc]], AtomicLong) = {
    val dir = Files.createTempDirectory(s"lightdb-bench-${spec.name.replace('+', '-')}-")
    val bench = spec.build(Some(dir))
    bench.db.init.sync()
    val (ids, counter) =
      if !preLoad then (Array.empty[Id[BenchDoc]], new AtomicLong(0L))
      else {
        val docs: Array[BenchDoc] = Array.tabulate(recordCount) { i =>
          BenchDoc(
            name = WorkloadData.nameFor(i),
            age = WorkloadData.ageFor(i),
            city = WorkloadData.cityFor(i),
            bio = WorkloadData.bioFor(i),
            _id = Id[BenchDoc](f"seed-$i%010d")
          )
        }
        bench.store.transaction(_.insert(docs.toIndexedSeq).unit).sync()
        (docs.map(_._id), new AtomicLong(recordCount.toLong))
      }
    (bench, dir, ids, counter)
  }

  def cleanup(bench: BenchDb, tempDir: Path): Unit = {
    if bench != null then bench.db.dispose.sync()
    if Files.exists(tempDir) then {
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
  }

  def randomId(ids: Array[Id[BenchDoc]]): Id[BenchDoc] =
    if ids.length == 0 then Id[BenchDoc]("missing")
    else ids(ThreadLocalRandom.current().nextInt(ids.length))

  def nextDoc(counter: AtomicLong): BenchDoc = {
    val n = counter.getAndIncrement()
    BenchDoc(
      name = WorkloadData.nameFor(n),
      age = WorkloadData.ageFor(n),
      city = WorkloadData.cityFor(n),
      bio = WorkloadData.bioFor(n),
      _id = Id[BenchDoc](f"upsert-$n%010d")
    )
  }
}

/** Master state for benchmarks that work against EVERY backend (all 17). */
@State(Scope.Benchmark)
class CompleteKvState {
  @Param(
    Array(
      "hashmap", "mapdb", "lmdb", "rocksdb", "halodb", "chroniclemap",
      "sqlite", "h2", "duckdb", "lucene", "tantivy",
      "rocksdb+lucene", "rocksdb+tantivy",
      "lmdb+lucene",    "lmdb+tantivy",
      "halodb+lucene",  "halodb+tantivy"
    )
  )
  var backend: String = _

  @Param(Array("100000"))
  var recordCount: Int = _

  protected var bench: BenchDb = _
  protected var tempDir: Path = _
  protected var preLoadedIds: Array[Id[BenchDoc]] = Array.empty
  protected var upsertCounter: AtomicLong = _

  def db: BenchDb = bench
  def randomId: Id[BenchDoc] = BenchSetup.randomId(preLoadedIds)
  def nextDoc(): BenchDoc = BenchSetup.nextDoc(upsertCounter)

  @Setup(Level.Trial)
  def setup(): Unit = {
    val (b, d, ids, c) = BenchSetup.init(BackendCatalog.find(backend), recordCount, preLoad = true)
    bench = b; tempDir = d; preLoadedIds = ids; upsertCounter = c
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = BenchSetup.cleanup(bench, tempDir)
}

/** State narrowed to backends with query support. Same setup, different `@Param` list. */
@State(Scope.Benchmark)
class CompleteCollectionState {
  @Param(
    Array(
      "sqlite", "h2", "duckdb", "lucene", "tantivy",
      "rocksdb+lucene", "rocksdb+tantivy",
      "lmdb+lucene",    "lmdb+tantivy",
      "halodb+lucene",  "halodb+tantivy"
    )
  )
  var backend: String = _

  @Param(Array("100000"))
  var recordCount: Int = _

  protected var bench: BenchDb = _
  protected var tempDir: Path = _
  protected var preLoadedIds: Array[Id[BenchDoc]] = Array.empty
  protected var upsertCounter: AtomicLong = _

  def db: BenchDb = bench
  def randomId: Id[BenchDoc] = BenchSetup.randomId(preLoadedIds)
  def nextDoc(): BenchDoc = BenchSetup.nextDoc(upsertCounter)

  def randomAgeRange(): (Int, Int) = {
    val center = WorkloadData.ageFor(ThreadLocalRandom.current().nextLong(recordCount.toLong))
    (center - 5, center + 5)
  }
  def randomName(): String =
    WorkloadData.nameFor(ThreadLocalRandom.current().nextLong(recordCount.toLong))
  def randomBioTerm(): String =
    WorkloadData.BioTokens(ThreadLocalRandom.current().nextInt(WorkloadData.BioTokens.length))

  @Setup(Level.Trial)
  def setup(): Unit = {
    val (b, d, ids, c) = BenchSetup.init(BackendCatalog.find(backend), recordCount, preLoad = true)
    bench = b; tempDir = d; preLoadedIds = ids; upsertCounter = c
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = BenchSetup.cleanup(bench, tempDir)
}

/** Bulk-insert state — empty backend at start of each iteration. */
@State(Scope.Benchmark)
class CompleteWriteState {
  @Param(
    Array(
      "hashmap", "mapdb", "lmdb", "rocksdb", "halodb", "chroniclemap",
      "sqlite", "h2", "duckdb", "lucene", "tantivy",
      "rocksdb+lucene", "rocksdb+tantivy",
      "lmdb+lucene",    "lmdb+tantivy",
      "halodb+lucene",  "halodb+tantivy"
    )
  )
  var backend: String = _

  @Param(Array("100000"))
  var recordCount: Int = _

  @Param(Array("buffered", "async"))
  var batch: String = _

  private var bench: BenchDb = _
  private var tempDir: Path = _
  var lastInsertCount: Int = 0

  @Setup(Level.Trial)
  def setup(): Unit = {
    val (b, d, _, _) = BenchSetup.init(BackendCatalog.find(backend), recordCount, preLoad = false)
    bench = b; tempDir = d
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = BenchSetup.cleanup(bench, tempDir)

  def runBulkInsert(): Unit = {
    val cfg = batch match {
      case "direct"   => lightdb.transaction.batch.BatchConfig.Direct
      case "buffered" => lightdb.transaction.batch.BatchConfig.Buffered()
      case "queued"   => lightdb.transaction.batch.BatchConfig.Queued()
      case "async"    => lightdb.transaction.batch.BatchConfig.Async()
      case other      => throw new IllegalArgumentException(s"unknown batch: $other")
    }
    val docs = (0 until recordCount).iterator.map { i =>
      BenchDoc(
        name = WorkloadData.nameFor(i),
        age = WorkloadData.ageFor(i),
        city = WorkloadData.cityFor(i),
        bio = WorkloadData.bioFor(i),
        _id = Id[BenchDoc](f"bulk-$i%010d")
      )
    }
    bench.store.transaction.withBatch(cfg).apply(_.insert(rapid.Stream.fromIterator(rapid.Task(docs))).unit).sync()
    lastInsertCount = recordCount
  }
}

/** Deterministic data generator. Same input → same output across backends. */
private[complete] object WorkloadData {
  private val firstNames = Array(
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry",
    "Iris", "James", "Karen", "Liam", "Mia", "Noah", "Olivia", "Peter"
  )
  private val cities = Array("Austin", "Boston", "Chicago", "Denver", "Eugene")

  val BioTokens: Array[String] = Array(
    "scala", "rust", "tantivy", "lucene", "search", "indexing", "fast", "query",
    "lightdb", "embedded", "kv", "collection", "split", "facet", "filter", "sort"
  )

  def nameFor(n: Long): String = firstNames((n % firstNames.length).toInt) + "-" + (n / firstNames.length)
  def ageFor(n: Long): Int = (n % 80).toInt + 18
  def cityFor(n: Long): Option[String] =
    if (n % 5 == 0) None else Some(cities((n % cities.length).toInt))
  def bioFor(n: Long): String = {
    val a = BioTokens((n % BioTokens.length).toInt)
    val b = BioTokens(((n / 16) % BioTokens.length).toInt)
    val c = BioTokens(((n / 256) % BioTokens.length).toInt)
    val d = BioTokens(((n / 4096) % BioTokens.length).toInt)
    s"$a $b $c $d"
  }
}
