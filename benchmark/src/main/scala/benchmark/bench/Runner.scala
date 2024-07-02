package benchmark.bench

import benchmark.bench.impl.{DerbyBench, H2Bench, NextBench, PostgreSQLBench, SQLiteBench, SQLiteJOOQBench, SQLiteTweaked2Bench}
import fabric.io.JsonFormatter
import fabric.rw.Convertible
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.{Files, Path}

object Runner {
  val implementations: Map[String, Bench] = Map(
//    "ldbHaloLucene" -> LightDBBench(HaloDBStore, LuceneIndexer),
//    "ldbMapLucene" -> LightDBBench(MapDBStore, LuceneIndexer),
//    "ldbRocksLucene" -> LightDBBench(RocksDBStore, LuceneIndexer),
//    "ldbAtomicLucene" -> LightDBBench(AtomicMapStore, LuceneIndexer),
//    "ldbMapLucene" -> LightDBBench(MapStore, LuceneIndexer),
//    "ldbHaloSQLite" -> LightDBBench(HaloDBStore, SQLiteIndexer),
//    "ldbHaloH2" -> LightDBBench(HaloDBStore, H2Indexer),
//    "ldbHaloDuck" -> LightDBBench(HaloDBStore, DuckDBIndexer),
    "SQLite" -> SQLiteBench,
    "SQLiteJOOQ" -> SQLiteJOOQBench,
    "SQLiteTweaked2" -> SQLiteTweaked2Bench,
    "PostgreSQL" -> PostgreSQLBench,
    "H2" -> H2Bench,
    "Derby" -> DerbyBench,
    "Next" -> NextBench
  )

  def main(args: Array[String]): Unit = {
    val dbDir = new File("db")
    FileUtils.deleteDirectory(dbDir)
    dbDir.mkdirs()

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
          val count = task.f(status)
          status.finish()
          if (count != task.maxProgress.toInt) {
            throw new RuntimeException(s"${bench.name} - ${task.name} expected ${task.maxProgress.toInt}, but received: $count")
          }
          val logs = status.logs
          scribe.info(s"Completed in ${logs.last.elapsed} seconds")
          BenchmarkReport(
            benchName = bench.name,
            name = task.name,
            maxProgress = task.maxProgress,
            size = bench.size(),
            logs = logs)
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
