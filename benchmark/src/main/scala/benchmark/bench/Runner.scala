package benchmark.bench

import benchmark.bench.impl._
import fabric.io.JsonFormatter
import fabric.rw._
import lightdb.h2.H2Store
import lightdb.halodb.HaloDBStore
import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.{Files, Path}

object Runner {
  val implementations: Map[String, Bench] = Map(
    "SQLite" -> SQLiteBench,
    "PostgreSQL" -> PostgreSQLBench,
    "H2" -> H2Bench,
    "Derby" -> DerbyBench,
    "MongoDB" -> MongoDBBench,
//    "LightDB011" -> LightDB011Bench
    "LightDB-SQLite" -> LightDBBench(SQLiteStore),
    "LightDB-Map-SQLite" -> LightDBBench(SplitStoreManager(HashMapStore, SQLiteStore)),
    "LightDB-HaloDB-SQLite" -> LightDBBench(SplitStoreManager(HaloDBStore, SQLiteStore)),
    "LightDB-Lucene" -> LightDBBench(LuceneStore),
    "LightDB-HaloDB-Lucene" -> LightDBBench(SplitStoreManager(HaloDBStore, LuceneStore)),
    "LightDB-RocksDB-Lucene" -> LightDBBench(SplitStoreManager(RocksDBStore, LuceneStore)),
    "LightDB-H2" -> LightDBBench(H2Store),
    "LightDB-HaloDB-H2" -> LightDBBench(SplitStoreManager(HaloDBStore, H2Store)),
    "LightDB-LMDB-Lucene" -> LightDBBench(LMDBStore), // SplitStoreManager(LMDBStore, LuceneStore)),
//    "LightDB-PostgreSQL" -> LightDBBench(PostgreSQLStoreManager(HikariConnectionManager(SQLConfig(
//      jdbcUrl = s"jdbc:postgresql://localhost:5432/basic",
//      username = Some("postgres"),
//      password = Some("password"),
//      maximumPoolSize = Some(100)
//    ))))
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
            scribe.warn(s"${bench.name} - ${task.name} expected ${task.maxProgress.toInt}, but received: $count")
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
