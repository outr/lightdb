package benchmark.jmh

import fabric.{obj, str}
import fabric.rw.*
import lightdb.*
import lightdb.doc.*
import lightdb.duckdb.DuckDBStore
import lightdb.h2.H2Store
import lightdb.id.Id
import lightdb.lmdb.LMDBStore
import lightdb.mapdb.MapDBStore
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.StoreManager
import lightdb.transaction.batch.BatchConfig
import lightdb.upgrade.DatabaseUpgrade
import org.openjdk.jmh.annotations.*
import rapid.{Stream, Task}

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

@State(Scope.Thread)
class ThroughputState {
  @Param(Array("rocksdb", "lmdb", "mapdb", "sqlite", "duckdb", "h2", "hashmap"))
  var backend: String = _

  @Param(Array("buffered"))
  var batchType: String = _

  @Param(Array("10000000"))
  var totalRecords: Int = _

  @Param(Array("64"))
  var valueSize: Int = _

  def run(): Unit = {
    val tempDir =
      if backend == "hashmap" then None
      else Some(Files.createTempDirectory("lightdb-throughput-"))

    val db = new ThroughputDb(selectStoreManager(backend), tempDir)
    try {
      db.init.sync()
      val batchConfig = selectBatchConfig(batchType)
      val value = str("x" * valueSize)

      val docs = Stream.fromIterator(Task {
        new Iterator[KeyValue] {
          private var i = 0
          override def hasNext: Boolean = i < totalRecords
          override def next(): KeyValue = {
            val key = f"key-$i%010d"
            i += 1
            KeyValue(Id[KeyValue](key), obj("v" -> value))
          }
        }
      })

      db.kv.transaction.withBatch(batchConfig).apply(_.insert(docs)).sync()
      db.kv.transaction.withBatch(batchConfig).apply(_.stream.drain).sync()
      db.kv.transaction.withBatch(batchConfig).apply { tx =>
        tx.stream.evalMap(kv => tx.delete(kv._id)).drain
      }.sync()
    } finally {
      db.dispose.sync()
      tempDir.foreach(deleteDirectory)
    }
  }

  private def selectStoreManager(name: String): StoreManager =
    name match {
      case "rocksdb" => RocksDBStore
      case "lmdb"    => LMDBStore
      case "mapdb"   => MapDBStore
      case "sqlite"  => SQLiteStore
      case "duckdb"  => DuckDBStore
      case "h2"      => H2Store
      case "hashmap" => HashMapStore
      case other     => throw new IllegalArgumentException(s"Unknown backend: $other")
    }

  private def selectBatchConfig(value: String): BatchConfig =
    value match {
      case "direct" => BatchConfig.Direct
      case "buffered" => BatchConfig.Buffered()
      case "queued" => BatchConfig.Queued()
      case "async" => BatchConfig.Async()
      case "storeNative" => BatchConfig.StoreNative
      case other => throw new IllegalArgumentException(s"Unknown batchType: $other")
    }

  private def deleteDirectory(path: Path): Unit = {
    if Files.exists(path) then {
      Files.walk(path).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
  }
}

class ThroughputBenchmark {
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1)
  def run(state: ThroughputState): Unit =
    state.run()
}

private class ThroughputDb[SM0 <: StoreManager](sm: SM0, dir: Option[Path]) extends LightDB {
  override type SM = SM0
  override val storeManager: SM = sm
  override val directory: Option[Path] = dir
  override def upgrades: List[DatabaseUpgrade] = Nil

  val kv: storeManager.S[KeyValue, KeyValue.type] = store(KeyValue, name = Some("kv"))
}
