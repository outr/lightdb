package benchmark.bench

import benchmark.bench.impl.LightDBBench
import fabric.io.JsonFormatter
import fabric.rw.Convertible
import lightdb.duckdb.DuckDBIndexer
import lightdb.h2.H2Indexer
import lightdb.halo.HaloDBStore
import lightdb.lucene.LuceneIndexer
import lightdb.mapdb.MapDBStore
import lightdb.rocks.RocksDBStore
import lightdb.sqlite.SQLiteIndexer
import lightdb.store.{AtomicMapStore, MapStore}

import java.nio.file.{Files, Path}

object Runner {
  val implementations: Map[String, Bench] = Map(
    "ldbHaloLucene" -> LightDBBench(HaloDBStore, LuceneIndexer),
    "ldbMapLucene" -> LightDBBench(MapDBStore, LuceneIndexer),
    "ldbRocksLucene" -> LightDBBench(RocksDBStore, LuceneIndexer),
    "ldbAtomicLucene" -> LightDBBench(AtomicMapStore, LuceneIndexer),
    "ldbMapLucene" -> LightDBBench(MapStore, LuceneIndexer),
    "ldbHaloSQLite" -> LightDBBench(HaloDBStore, SQLiteIndexer),
    "ldbHaloH2" -> LightDBBench(HaloDBStore, H2Indexer),
    "ldbHaloDuck" -> LightDBBench(HaloDBStore, DuckDBIndexer),
  )

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some(implName) if implementations.contains(implName) =>
        val bench = implementations(implName)
        scribe.info(s"Initializing $implName benchmark...")
        bench.init()
        scribe.info(s"Initialized successfully!")
        val reports = bench.tasks.map { task =>
          val status = StatusCallback()
          status.start()
          scribe.info(s"Executing ${task.name} task...")
          task.f(status)
          status.finish()
          val logs = status.logs
          scribe.info(s"Completed in ${logs.last.elapsed} seconds")
          BenchmarkReport(task.name, task.maxProgress, logs)
        }
        scribe.info(s"Disposing $implName benchmark...")
        bench.dispose()
        scribe.info(s"Disposed!")

        val json = reports.json
        Files.writeString(Path.of(s"report-$implName.json"), JsonFormatter.Default(json))

        sys.exit(0)
      case Some(implName) => scribe.error(s"Invalid implementation name: $implName. Valid implementations: ${implementations.keys.mkString(", ")}")
      case None => scribe.error(s"Exactly one command-line argument must be present to specify the implementation. Valid implementations: ${implementations.keys.mkString(", ")}")
    }
  }
}
