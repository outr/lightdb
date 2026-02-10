package benchmark.jmh.imdb

import lightdb.*
import lightdb.doc.DocumentModel
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.mapdb.MapDBStore
import lightdb.rocksdb.RocksDBStore
import lightdb.lmdb.LMDBStore
import lightdb.h2.H2Store
import lightdb.sql.SQLiteStore
import lightdb.duckdb.DuckDBStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{Collection, CollectionManager, Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.openjdk.jmh.annotations.*
import rapid.Task

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

@State(Scope.Benchmark)
class ImdbState {
  @Param(Array("rocksdb", "lmdb", "mapdb", "sqlite", "duckdb", "h2", "halodb", "rocksdb-lucene"))
  var backend: String = _

  @Param(Array("50000"))
  var recordLimit: Int = _

  @Param(Array("1000"))
  var batchSize: Int = _

  @Param(Array("data")) // directory containing title.akas.tsv and title.basics.tsv
  var dataDir: String = _

  private var tempDir: Option[Path] = None
  private var db: ImdbDb[_] = _
  private var akaIds: Array[String] = Array.empty
  private var titleIds: Array[String] = Array.empty

  @Setup(Level.Trial)
  def setup(): Unit = {
    val dir = backend match {
      case "hashmap" => None
      case _ => Some(Files.createTempDirectory("imdb-bench-"))
    }
    tempDir = dir
    db = ImdbDb.create(backend, dir)
    db.init.sync()

    val dataPath = Path.of(dataDir)
    val akasFile = dataPath.resolve("title.akas.tsv").toFile
    val basicsFile = dataPath.resolve("title.basics.tsv").toFile

    val (akas, basics) =
      if akasFile.exists() && basicsFile.exists() then {
        (loadAkas(akasFile, recordLimit), loadBasics(basicsFile, recordLimit))
      } else {
        synthetic(recordLimit)
      }

    ingest(akas, basics)
    akaIds = akas.map(_. _1).toArray
    titleIds = akas.map(_._2).toArray.distinct
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    if db != null then db.dispose.sync()
    tempDir.foreach { p =>
      if Files.exists(p) then {
        Files.walk(p).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      }
    }
  }

  private def ingest(akas: Seq[(String, String, TitleAka)], basics: Seq[TitleBasics]): Unit = {
    // insert basics
    basics.grouped(batchSize).foreach { chunk =>
      db.basics.transaction(_.insert(chunk)).sync()
    }
    // insert akas
    akas.grouped(batchSize).foreach { chunk =>
      db.aka.transaction(_.insert(chunk.map(_._3))).sync()
    }
  }

  private def loadAkas(file: File, limit: Int): Seq[(String, String, TitleAka)] = {
    val reader = new BufferedReader(new FileReader(file))
    try {
      val header = reader.readLine().split('\t').toList
      val buf = ArrayBuffer.empty[(String, String, TitleAka)]
      var line = reader.readLine()
      while line != null && buf.length < limit do {
        val cols = line.split('\t').toList
        val map = header.zip(cols).filter(_._2.nonEmpty).toMap
        val titleId = map.getOrElse("titleId", "")
        val ordering = map.get("ordering").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(0)
        val title = map.getOrElse("title", "")
        val region = map.get("region").filterNot(_.isEmpty)
        val language = map.get("language").filterNot(_.isEmpty)
        val types = map.get("types").map(_.split(',').toList).getOrElse(Nil)
        val attrs = map.get("attributes").map(_.split(',').toList).getOrElse(Nil)
        val isOriginal = map.get("isOriginalTitle").flatMap(s => scala.util.Try(s.toInt != 0).toOption)
        val tconst = map.getOrElse("titleId", "")
        val aka = TitleAka(
          titleId = tconst,
          ordering = ordering,
          title = title,
          region = region,
          language = language,
          types = types,
          attributes = attrs,
          isOriginalTitle = isOriginal,
          _id = Id[TitleAka](map.getOrElse("titleId", "") + "-" + ordering)
        )
        buf += ((aka._id.value, tconst, aka))
        line = reader.readLine()
      }
      buf.toSeq
    } finally {
      reader.close()
    }
  }

  private def loadBasics(file: File, limit: Int): Seq[TitleBasics] = {
    val reader = new BufferedReader(new FileReader(file))
    try {
      val header = reader.readLine().split('\t').toList
      val buf = ArrayBuffer.empty[TitleBasics]
      var line = reader.readLine()
      while line != null && buf.length < limit do {
        val cols = line.split('\t').toList
        val map = header.zip(cols).filter(_._2.nonEmpty).toMap
        val basics = TitleBasics(
          tconst = map.getOrElse("tconst", ""),
          titleType = map.getOrElse("titleType", ""),
          primaryTitle = map.getOrElse("primaryTitle", ""),
          originalTitle = map.getOrElse("originalTitle", ""),
          isAdult = map.get("isAdult").exists(_.toIntOption.contains(1)),
          startYear = map.get("startYear").flatMap(_.toIntOption).getOrElse(0),
          endYear = map.get("endYear").flatMap(_.toIntOption).getOrElse(0),
          runtimeMinutes = map.get("runtimeMinutes").flatMap(_.toIntOption).getOrElse(0),
          genres = map.get("genres").map(_.split(',').toList).getOrElse(Nil)
        )
        buf += basics
        line = reader.readLine()
      }
      buf.toSeq
    } finally {
      reader.close()
    }
  }

  private def synthetic(limit: Int): (Seq[(String, String, TitleAka)], Seq[TitleBasics]) = {
    val akas = (0 until limit).map { i =>
      val id = f"tt$i%08d"
      val akaId = s"$id-$i"
      val aka = TitleAka(
        titleId = id,
        ordering = i,
        title = s"title-$i",
        region = None,
        language = None,
        types = Nil,
        attributes = Nil,
        isOriginalTitle = Some(true),
        _id = Id[TitleAka](akaId)
      )
      (akaId, id, aka)
    }
    val basics = (0 until limit).map { i =>
      val id = f"tt$i%08d"
      TitleBasics(
        tconst = id,
        titleType = "movie",
        primaryTitle = s"primary-$i",
        originalTitle = s"original-$i",
        isAdult = false,
        startYear = 2000 + (i % 20),
        endYear = 0,
        runtimeMinutes = 90 + (i % 30),
        genres = List("Drama")
      )
    }
    (akas, basics)
  }

  def randomGetAka(): Option[TitleAka] = {
    val idx = ThreadLocalRandom.current().nextInt(akaIds.length)
    db.aka.transaction(_.get(Id[TitleAka](akaIds(idx)))).sync()
  }

  def randomFindByTitleId(): List[TitleAka] = {
    val idx = ThreadLocalRandom.current().nextInt(titleIds.length)
    val titleId = titleIds(idx)
    db.aka.transaction(_.stream.filter(_.titleId == titleId).toList).sync()
  }
}

private trait ImdbDb[SM] extends LightDB {
  val aka: Store[TitleAka, TitleAka.type]
  val basics: Store[TitleBasics, TitleBasics.type]
}

private object ImdbDb {
  def create(backend: String, dir: Option[Path]): ImdbDb[_] = backend match {
    case "rocksdb" => new Rocks(dir, RocksDBStore)
    case "lmdb"    => new Rocks(dir, LMDBStore)
    case "mapdb"   => new Rocks(dir, MapDBStore)
    case "sqlite"  => new SqlDb(SQLiteStore, dir)
    case "duckdb"  => new SqlDb(DuckDBStore, dir)
    case "h2"      => new SqlDb(H2Store, dir)
    case "halodb"  => new SqlDb(HaloDBStore, dir)
    case "hashmap" => new Rocks(None, HashMapStore)
    case "rocksdb-lucene" =>
      new SplitDb(SplitStoreManager(RocksDBStore, LuceneStore), dir)
    case other => throw new IllegalArgumentException(s"Unknown backend: $other")
  }

  private class Rocks[SMParam <: StoreManager](dir: Option[Path], val sm: SMParam) extends ImdbDb[SMParam] {
    override type SM = SMParam
    override val storeManager: SMParam = sm
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }

  private class SqlDb[SMParam <: CollectionManager](val sm: SMParam, dir: Option[Path]) extends ImdbDb[SMParam] {
    override type SM = SMParam
    override val storeManager: SMParam = sm
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }

  private class SplitDb(sm: SplitStoreManager[RocksDBStore.type, LuceneStore.type], dir: Option[Path]) extends ImdbDb[SplitStoreManager[RocksDBStore.type, LuceneStore.type]] {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStore.type]
    override val storeManager: SM = sm
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }
}

