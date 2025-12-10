package benchmark.jmh

import fabric.rw._
import fabric.{Json, obj, str}
import lightdb._
import lightdb.doc._
import lightdb.id.Id
import lightdb.rocksdb.RocksDBStore
import lightdb.lmdb.LMDBStore
import lightdb.mapdb.MapDBStore
import lightdb.sql.SQLiteStore
import lightdb.h2.H2Store
import lightdb.duckdb.DuckDBStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import rapid.Task

import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

@State(Scope.Benchmark)
class KvState {
  @Param(Array("rocksdb", "lmdb", "mapdb", "sqlite", "duckdb", "h2", "hashmap"))
  var backend: String = _

  @Param(Array("10000"))
  var preLoad: Int = _

  @Param(Array("64"))
  var valueSize: Int = _

  private var tempDir: Option[Path] = None
  private var db: KVDb[_ <: StoreManager] = _
  private var keys: Array[String] = Array.empty
  private val upsertCounter = new AtomicInteger(0)

  @Setup(Level.Trial)
  def setup(): Unit = {
    val dir = Files.createTempDirectory("lightdb-bench-")
    tempDir = Some(dir)

    db = backend match {
      case "rocksdb" => new KVDb(RocksDBStore, tempDir)
      case "lmdb"    => new KVDb(LMDBStore, tempDir)
      case "mapdb"   => new KVDb(MapDBStore, tempDir)
      case "sqlite"  => new KVDb(SQLiteStore, tempDir)
      case "duckdb"  => new KVDb(DuckDBStore, tempDir)
      case "h2"      => new KVDb(H2Store, tempDir)
      case "hashmap" => new KVDb(HashMapStore, None)
      case other     => throw new IllegalArgumentException(s"Unknown backend: $other")
    }

    db.init.sync()

    // Preload data
    val preKeys = (0 until preLoad).map(i => f"key-$i%08d").toArray
    keys = preKeys
    val value = str("x" * valueSize)
    val docs = preKeys.map { k =>
      KeyValue(Id[KeyValue](k), obj("v" -> value))
    }
    db.kv.transaction(_.insert(docs)).sync()
    upsertCounter.set(preLoad)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    if (db != null) db.dispose.sync()
    tempDir.foreach { p =>
      if (Files.exists(p)) {
        Files.walk(p).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      }
    }
  }

  def randomKey: String = {
    val idx = ThreadLocalRandom.current().nextInt(keys.length)
    keys(idx)
  }

  def nextKey: String = {
    val id = upsertCounter.getAndIncrement()
    f"key-$id%08d"
  }

  def get(): Option[KeyValue] =
    db.kv.transaction(_.get(Id[KeyValue](randomKey))).sync()

  def upsert(): KeyValue = {
    val k = nextKey
    val doc = KeyValue(Id[KeyValue](k), obj("v" -> str("x" * valueSize)))
    db.kv.transaction(_.upsert(doc)).sync()
    doc
  }
}

private class KVDb[SM0 <: StoreManager](sm: SM0, dir: Option[Path]) extends LightDB {
  override type SM = SM0
  override val storeManager: SM = sm
  override val directory: Option[Path] = dir
  override def upgrades: List[DatabaseUpgrade] = Nil
  val kv: storeManager.S[KeyValue, KeyValue.type] = store(KeyValue, name = Some("kv"))
}

