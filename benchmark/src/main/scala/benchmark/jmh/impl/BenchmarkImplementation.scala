package benchmark.jmh.impl

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.h2.H2Store
import lightdb.halodb.HaloDBStore
import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import rapid.Task

trait BenchmarkImplementation {
  def init: Task[Unit]

  def insert(iterations: Int): Task[Unit]

  def count: Task[Int]

  def read: Task[Int]

  def traverse: Task[Unit]

  def dispose: Task[Unit]
}

object BenchmarkImplementation {
  def apply(name: String): BenchmarkImplementation = name match {
    case "RocksDB" => RocksDBImpl()
    case "SQLite" => SQLiteImpl()
    case "LightDB-RocksDB" => LightDBImpl(RocksDBStore)
    case "LightDB-HaloDB" => LightDBImpl(HaloDBStore)
    case "LightDB-H2" => LightDBImpl(H2Store)
    case "LightDB-LMDB" => LightDBImpl(LMDBStore)
    case "LightDB-ChronicleMap" => LightDBImpl(ChronicleMapStore)
    case "LightDB-SQLite" => LightDBImpl(SQLiteStore)
    case "LightDB-Lucene" => LightDBImpl(LuceneStore)
    case other => throw new IllegalArgumentException(s"Unsupported backend: $other")
  }
}