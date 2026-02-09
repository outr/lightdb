package benchmark.jmh.imdb

import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, CollectionManager, Store, StoreManager}
import lightdb.store.split.SplitStoreManager
import lightdb.traversal.store.TraversalStore
import lightdb.upgrade.DatabaseUpgrade
import org.openjdk.jmh.annotations.*

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

@State(Scope.Benchmark)
class ImdbQueryComparisonState {
  @Param(Array("sqlite", "rocksdb-lucene", "traversal"))
  var backend: String = _

  @Param(Array("50000"))
  var recordLimit: Int = _

  @Param(Array("1000"))
  var batchSize: Int = _

  @Param(Array("data")) // directory containing title.akas.tsv and title.basics.tsv
  var dataDir: String = _

  // With chunked In streaming, the In filter now processes values in groups of
  // streamingMaxInTerms (default 1024) via flatMap, so large In lists are handled
  // without falling back to a full scan.
  private val scopedTitleIdSampleSize: Int = 2000

  private var tempDir: Option[Path] = None
  private var db: ImdbQueryDb[_] = _
  private var akaIds: Array[String] = Array.empty
  private var titleIds: Array[String] = Array.empty

  @Setup(Level.Trial)
  def setup(): Unit = {
    val dir = backend match {
      case _ => Some(Files.createTempDirectory("imdb-query-bench-"))
    }
    tempDir = dir
    db = ImdbQueryDb.create(backend, dir)
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
    akaIds = akas.map(_._1).toArray
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
    basics.grouped(batchSize).foreach { chunk =>
      db.basics.transaction(_.insert(chunk)).sync()
    }
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
        region = if i % 3 == 0 then Some("US") else if i % 5 == 0 then Some("GB") else None,
        language = if i % 4 == 0 then Some("en") else if i % 7 == 0 then Some("de") else None,
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

  private def sampleTitleIds(n: Int): List[String] = {
    if titleIds.isEmpty then Nil
    else if titleIds.length <= n then titleIds.toList
    else {
      val maxStart = titleIds.length - n
      val start = if maxStart <= 0 then 0 else ThreadLocalRandom.current().nextInt(maxStart)
      titleIds.slice(start, start + n).toList
    }
  }

  // ---------------------------------------------------------------------------
  // Benchmark 1 – Indexed exact-match query (parity benchmark)
  //
  // Uses tx.query.filter() which leverages the indexed titleId field.
  // Expected: all backends use their respective index structures (B-tree, inverted
  // index, persisted postings) and should show similar throughput.
  // ---------------------------------------------------------------------------
  def indexedFilterEquals(): List[TitleAka] = {
    val idx = ThreadLocalRandom.current().nextInt(titleIds.length)
    val titleId = titleIds(idx)
    db.aka.transaction { tx =>
      tx.query
        .filter(_.titleId === titleId)
        .toList
    }.sync()
  }

  // ---------------------------------------------------------------------------
  // Benchmark 2 – Substring contains (traversal advantage)
  //
  // Standalone contains on an indexed field. The needle is >= 3 chars so
  // traversal can use n-gram postings to seed candidates efficiently, avoiding
  // a full scan. SQLite falls back to LIKE '%...%' (no B-tree usage) and
  // Lucene may use wildcard/n-gram depending on configuration.
  // ---------------------------------------------------------------------------
  def containsQuery(): List[TitleAka] = {
    db.aka.transaction { tx =>
      tx.query
        .filter(_.title.contains("title-123"))
        .limit(200)
        .toList
    }.sync()
  }

  // ---------------------------------------------------------------------------
  // Benchmark 3 – Prefix startsWith (traversal advantage / parity)
  //
  // Prefix queries map directly to persisted prefix postings in traversal.
  // SQLite can use the B-tree index (LIKE 'prefix%'), and Lucene uses its
  // prefix query path.
  // ---------------------------------------------------------------------------
  def startsWithQuery(): List[TitleAka] = {
    db.aka.transaction { tx =>
      tx.query
        .filter(_.title.startsWith("title-1"))
        .limit(200)
        .toList
    }.sync()
  }

  // ---------------------------------------------------------------------------
  // Benchmark 4 – Small scoped In + contains (traversal streaming seed)
  //
  // A small In scope (<=32 terms) lets traversal use its streaming-seed
  // optimisation for Filter.Multi: the In clause drives a streaming postings
  // lookup, and the contains is verified on the fly. SQLite does an IN(...) +
  // LIKE '%...%' query; Lucene does a boolean query.
  // ---------------------------------------------------------------------------
  def scopedContainsQuery(): List[TitleAka] = {
    val scopedTitleIds = sampleTitleIds(scopedTitleIdSampleSize)
    if scopedTitleIds.isEmpty then Nil
    else {
      db.aka.transaction { tx =>
        tx.query
          .filter(_.titleId.in(scopedTitleIds))
          .filter(_.title.contains("title-1"))
          .limit(200)
          .toList
      }.sync()
    }
  }
}

private trait ImdbQueryDb[SM] extends LightDB {
  val aka: Store[TitleAka, TitleAka.type]
  val basics: Store[TitleBasics, TitleBasics.type]
}

private object ImdbQueryDb {
  def create(backend: String, dir: Option[Path]): ImdbQueryDb[_] = backend match {
    case "sqlite" => new SqlDb(SQLiteStore, dir)
    case "rocksdb-lucene" =>
      new SplitDb(SplitStoreManager(RocksDBStore, LuceneStore), dir)
    case "traversal" => new TraversalDb(dir)
    case other => throw new IllegalArgumentException(s"Unknown backend: $other")
  }

  private class SqlDb[SMParam <: CollectionManager](val sm: SMParam, dir: Option[Path]) extends ImdbQueryDb[SMParam] {
    override type SM = SMParam
    override val storeManager: SMParam = sm
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }

  private class SplitDb(sm: SplitStoreManager[RocksDBStore.type, LuceneStore.type], dir: Option[Path])
    extends ImdbQueryDb[SplitStoreManager[RocksDBStore.type, LuceneStore.type]] {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStore.type]
    override val storeManager: SM = sm
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }

  private class TraversalDb(dir: Option[Path]) extends ImdbQueryDb[CollectionManager] {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = new CollectionManager {
      override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] =
        TraversalStore[Doc, Model]

      override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
        db: lightdb.LightDB,
        model: Model,
        name: String,
        path: Option[java.nio.file.Path],
        storeMode: lightdb.store.StoreMode[Doc, Model]
      ): S[Doc, Model] = {
        val backing = RocksDBStore.create(db, model, s"${name}__backing", path, storeMode)
        val indexPath = path.map(p => p.getParent.resolve(s"${name}__tindex"))
        val indexBacking =
          RocksDBStore.create(db, lightdb.KeyValue, s"${name}__tindex", indexPath, lightdb.store.StoreMode.All())
        new TraversalStore[Doc, Model](
          name = name,
          path = path,
          model = model,
          backing = backing,
          indexBacking = Some(indexBacking),
          lightDB = db,
          storeManager = this
        )
      }
    }
    override val directory: Option[Path] = dir
    override def upgrades: List[DatabaseUpgrade] = Nil
    val aka: Store[TitleAka, TitleAka.type] = store(TitleAka, name = Some("aka"))
    val basics: Store[TitleBasics, TitleBasics.type] = store(TitleBasics, name = Some("basics"))
  }
}
