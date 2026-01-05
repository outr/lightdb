package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.filter._
import lightdb.field.Field
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import profig.Profig
import fabric.rw._
import rapid.{AsyncTaskSpec, Task}

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.{Files, Path}
import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Abstract, opt-in performance spec for large IMDB-like datasets.
 *
 * Enable with:
 * -Dimdb.perf=true
 *
 * Configure with:
 * -Dimdb.perf.records=500000
 * -Dimdb.perf.batch=5000
 * -Dimdb.perf.dataDir=data   (expects title.akas.tsv and title.basics.tsv; otherwise uses synthetic)
 */
abstract class AbstractImdbPerformanceSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected def storeManager: CollectionManager

  protected def enabled: Boolean = Profig("imdb.perf").opt[Boolean].getOrElse(false)
  protected def recordLimit: Int = Profig("imdb.perf.records").opt[Int].getOrElse(50_000)
  protected def batchSize: Int = Profig("imdb.perf.batch").opt[Int].getOrElse(5_000)
  protected def dataDir: String = Profig("imdb.perf.dataDir").opt[String].getOrElse("data")
  protected def hopLimit: Int = Profig("imdb.perf.hopLimit").opt[Int].getOrElse(10_000)
  protected def hopDepth: Int = Profig("imdb.perf.hopDepth").opt[Int].getOrElse(3)

  protected lazy val specName: String = getClass.getSimpleName

  // Use a temp directory per run (for RocksDB-backed tests).
  private var dir: Path = _
  protected var db: DB = _

  private def requireEnabled(): Unit =
    if (!enabled) cancel("IMDB performance spec is disabled. Run with -Dimdb.perf=true")

  specName should {
    "setup (load IMDB dataset)" in {
      Task {
        requireEnabled()
        dir = Files.createTempDirectory("lightdb-imdb-perf-")
        db = new DB(dir)
      }.flatMap(_ => db.init).flatMap { _ =>
        Task {
          val dataPath = Path.of(dataDir)
          val akasFile = dataPath.resolve("title.akas.tsv").toFile
          val basicsFile = dataPath.resolve("title.basics.tsv").toFile

          val (akas, basics) =
            if (akasFile.exists() && basicsFile.exists()) {
              (loadAkas(akasFile, recordLimit), loadBasics(basicsFile, recordLimit))
            } else {
              synthetic(recordLimit)
            }

          val start = System.nanoTime()
          basics.grouped(batchSize).foreach(chunk => db.basics.transaction(_.insert(chunk)).sync())
          akas.grouped(batchSize).foreach(chunk => db.aka.transaction(_.insert(chunk)).sync())

          // Build synthetic graph edges for deeper traversal-style queries.
          val ids = basics.map(_._id).toVector
          val links = ids.indices.flatMap { i =>
            val from = ids(i)
            // two deterministic neighbors per node (simple ring + skip) to keep generation cheap
            val to1 = ids((i + 1) % ids.length)
            val to2 = ids((i + 97) % ids.length)
            List(
              PerfTitleLink(fromId = from, toId = to1, kind = "related"),
              PerfTitleLink(fromId = from, toId = to2, kind = "related")
            )
          }
          links.grouped(batchSize).foreach(chunk => db.links.transaction(_.insert(chunk)).sync())

          // Basic sanity: counts match inserted (this validates inserts + backing store correctness).
          db.basics.transaction(_.count).sync() shouldBe basics.size
          db.aka.transaction(_.count).sync() shouldBe akas.size
          db.links.transaction(_.count).sync() shouldBe links.size

          val tookMs = (System.nanoTime() - start) / 1_000_000L
          scribe.info(s"IMDB perf dataset loaded: akas=${akas.size}, basics=${basics.size}, links=${links.size}, took=${tookMs}ms, dir=$dir")
        }
      }.succeed
    }

    "run traversal-style query pipeline (scoped -> narrow -> hop -> facet/count)" in {
      Task.defer {
        requireEnabled()

        // Query 1: Narrow basics by startYear range + runtime + genre membership
        val q1 = db.basics.transaction { tx =>
          val start = System.nanoTime()
          tx.query
            .filter(b => b.startYear >= 2005 && b.startYear <= 2015)
            .filter(_.runtimeMinutes >= 90)
            .filter(_.genres.hasAny(List("Drama", "Comedy")))
            .id
            .limit(5_000) // keep hop manageable; increase for stress
            .toList
            .map { ids =>
              val tookMs = (System.nanoTime() - start) / 1_000_000L
              scribe.info(s"Q1 basics narrow -> ids=${ids.size}, took=${tookMs}ms")
              ids
            }
        }

        // Query 2: Hop into aka by titleId and facet by region/language
        q1.flatMap { basicsIds =>
          val sampledTitleIdsSet = basicsIds.take(2_000).map(_.value).toSet
          val sampledTitleIds = sampledTitleIdsSet.toList
          db.aka.transaction { tx =>
            val start = System.nanoTime()
            tx.query
              .filter(_.titleId.in(sampledTitleIds))
              .facet(_.regionFacet, childrenLimit = Some(25))
              .facet(_.languageFacet, childrenLimit = Some(25))
              .countTotal(true)
              .limit(50)
              .search
              .flatMap { results =>
                results.list.map { docs =>
                  val tookMs = (System.nanoTime() - start) / 1_000_000L
                  val total = results.total.getOrElse(-1)
                  val regionFacet = results.facet(_.regionFacet)
                  val languageFacet = results.facet(_.languageFacet)
                  val regions = regionFacet.values.take(10)
                  val langs = languageFacet.values.take(10)

                  // Validate scoping: all returned docs must be within the hop-scope.
                  docs.foreach(d => sampledTitleIdsSet.contains(d.titleId) shouldBe true)

                  // Validate totals + facet invariants (non-brittle across datasets).
                  results.total.isDefined shouldBe true
                  total should be >= docs.size
                  regionFacet.values.size should be <= 25
                  languageFacet.values.size should be <= 25
                  regionFacet.childCount should be >= regionFacet.values.size
                  languageFacet.childCount should be >= languageFacet.values.size
                  // FacetResult.totalCount is "sum of counts" (can differ from total docs when missing values / multi-valued facets).
                  regionFacet.totalCount shouldBe regionFacet.values.map(_.count).sum
                  languageFacet.totalCount shouldBe languageFacet.values.map(_.count).sum
                  // These two facets are configured as single-valued; so totalCount can't exceed total matching docs.
                  regionFacet.totalCount should be <= total
                  languageFacet.totalCount should be <= total

                  scribe.info(s"Q2 aka hop -> total=$total, took=${tookMs}ms, topRegions=${regions}, topLangs=${langs}")
                  succeed
                }
              }
          }
        }
      }
    }

    "run deeper multi-hop + ExistsChild-style pipelines" in {
      Task.defer {
        requireEnabled()

        // Step A: seed a moderately-sized set of basics ids (narrowed)
        val seedIdsT = db.basics.transaction { tx =>
          val start = System.nanoTime()
          tx.query
            .filter(b => b.startYear >= 2000 && b.startYear <= 2019)
            .filter(_.runtimeMinutes >= 80)
            .filter(_.genres.hasAny(List("Drama", "Comedy")))
            .id
            .limit(hopLimit)
            .toList
            .map { ids =>
              val tookMs = (System.nanoTime() - start) / 1_000_000L
              scribe.info(s"MultiHop.A seed basics -> ids=${ids.size}, took=${tookMs}ms")
              ids
            }
        }

        // Step B: multi-hop through links (simulate graph traversal)
        val reachableT = seedIdsT.flatMap { seed =>
          Task {
            val start = System.nanoTime()
            val seedSet = seed.toSet
            var frontier: Set[Id[PerfTitleBasics]] = seedSet
            var visited: Set[Id[PerfTitleBasics]] = seedSet
            var depth = 0
            while (depth < hopDepth && frontier.nonEmpty && visited.size < hopLimit * 5) {
              val next = db.links.transaction { tx =>
                tx.query
                  .filter(_.fromId.in(frontier.toList))
                  .value(_.toId)
                  .toList
              }.sync().toSet
              val newFrontier = next.diff(visited)
              visited = visited ++ newFrontier
              frontier = newFrontier
              depth += 1
            }

            // Validate traversal correctness (non-brittle): visited must include the seed set.
            visited.intersect(seedSet) shouldBe seedSet
            visited.size should be >= seedSet.size

            val tookMs = (System.nanoTime() - start) / 1_000_000L
            scribe.info(s"MultiHop.B BFS -> visited=${visited.size}, depth=$depth, took=${tookMs}ms")
            visited.take(hopLimit * 2).toList
          }
        }

        // Step C: ExistsChild-style semi-join (child predicate -> parent ids -> parent filter)
        reachableT.flatMap { reachable =>
          val reachableSet = reachable.toSet
          val childToParentIdsT: Task[List[Id[PerfTitleBasics]]] = db.aka.transaction { tx =>
            val start = System.nanoTime()
            tx.query
              .filter(_.basicsId.in(reachableSet.toList))
              .filter(a => a.region === Some("US") && a.language === Some("en"))
              .value(_.basicsId)
              .toList
              .map { ids =>
                val tookMs = (System.nanoTime() - start) / 1_000_000L
                val distinct = ids.distinct
                // Validate: semi-join parent ids must be within the reachable set we scoped to.
                distinct.foreach(id => reachableSet.contains(id) shouldBe true)
                scribe.info(s"MultiHop.C1 child predicate -> parentIds=${distinct.size}, took=${tookMs}ms")
                distinct
              }
          }

          childToParentIdsT.flatMap { parentIds =>
            val parentIdSet = parentIds.toSet
            db.basics.transaction { tx =>
              val start = System.nanoTime()
              tx.query
                .filter(_.tconst.in(parentIds.map(_.value)))
                .sort(lightdb.Sort.ByField(PerfTitleBasics.startYear).desc)
                .limit(500)
                .id
                .toList
                .map { ids =>
                  val tookMs = (System.nanoTime() - start) / 1_000_000L
                  // Validate: parent results are a subset of parentIds.
                  ids.foreach(id => parentIdSet.contains(id) shouldBe true)
                  scribe.info(s"MultiHop.C2 parents after semi-join -> ids=${ids.size}, took=${tookMs}ms")
                  ids
                }
            }.flatMap { ids =>
              db.aka.transaction { tx =>
                val start = System.nanoTime()
                tx.query
                  .filter(_.basicsId.in(ids))
                  .facet(_.regionFacet, childrenLimit = Some(25))
                  .facet(_.languageFacet, childrenLimit = Some(25))
                  .countTotal(true)
                  .limit(50)
                  .search
                  .flatMap { results =>
                    val allowedBasicsIds = ids.toSet
                    results.list.map { docs =>
                      // Validate: all returned docs remain within narrowed parent set.
                      docs.foreach(d => allowedBasicsIds.contains(d.basicsId) shouldBe true)

                      val tookMs = (System.nanoTime() - start) / 1_000_000L
                      val total = results.total.getOrElse(-1)
                      val regionFacet = results.facet(_.regionFacet)
                      val languageFacet = results.facet(_.languageFacet)
                      results.total.isDefined shouldBe true
                      total should be >= docs.size
                      regionFacet.values.size should be <= 25
                      languageFacet.values.size should be <= 25
                      regionFacet.totalCount shouldBe regionFacet.values.map(_.count).sum
                      languageFacet.totalCount shouldBe languageFacet.values.map(_.count).sum
                      regionFacet.totalCount should be <= total
                      languageFacet.totalCount should be <= total

                      scribe.info(s"MultiHop.D aka facets over narrowed parents -> total=$total, took=${tookMs}ms")
                      succeed
                    }
                  }
              }
            }
          }
        }
      }
    }

    "run scoped contains pipeline (prefilter + verify)" in {
      Task.defer {
        requireEnabled()

        // Seed a scoped set of titleIds from basics, then apply a contains filter over aka.title within the scope.
        val seedTitleIdsT = db.basics.transaction { tx =>
          tx.query
            .filter(b => b.startYear >= 2000 && b.startYear <= 2019)
            .id
            .limit(2_000)
            .toList
            .map(_.map(_.value).toSet)
        }

        seedTitleIdsT.flatMap { seedIds =>
          val needle = "title-1" // works for synthetic; for real IMDB data, may yield 0 which is fine
          db.aka.transaction { tx =>
            val start = System.nanoTime()
            tx.query
              .filter(_.titleId.in(seedIds.toList))
              .filter(_.title.contains(needle))
              .countTotal(true)
              .limit(200)
              .search
              .flatMap { results =>
                results.list.map { docs =>
                  val tookMs = (System.nanoTime() - start) / 1_000_000L
                  val total = results.total.getOrElse(-1)
                  // Validate scoping + correctness.
                  docs.foreach(d => seedIds.contains(d.titleId) shouldBe true)
                  docs.foreach(d => d.title.toLowerCase.contains(needle) shouldBe true)
                  total should be >= docs.size
                  scribe.info(s"Contains.Scoped aka.title contains('$needle') -> total=$total, returned=${docs.size}, took=${tookMs}ms")
                  succeed
                }
              }
          }
        }
      }
    }
  }

  // ---- data loading helpers (mirrors benchmark ImdbState) ----
  private def loadAkas(file: File, limit: Int): Seq[PerfTitleAka] = {
    val reader = new BufferedReader(new FileReader(file))
    try {
      val header = reader.readLine().split('\t').toList
      val buf = ArrayBuffer.empty[PerfTitleAka]
      var line = reader.readLine()
      while (line != null && buf.length < limit) {
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
        buf += PerfTitleAka(
          titleId = titleId,
          basicsId = Id[PerfTitleBasics](titleId),
          ordering = ordering,
          title = title,
          region = region,
          language = language,
          types = types,
          attributes = attrs,
          isOriginalTitle = isOriginal,
          _id = Id[PerfTitleAka](titleId + "-" + ordering)
        )
        line = reader.readLine()
      }
      buf.toSeq
    } finally {
      reader.close()
    }
  }

  private def loadBasics(file: File, limit: Int): Seq[PerfTitleBasics] = {
    val reader = new BufferedReader(new FileReader(file))
    try {
      val header = reader.readLine().split('\t').toList
      val buf = ArrayBuffer.empty[PerfTitleBasics]
      var line = reader.readLine()
      while (line != null && buf.length < limit) {
        val cols = line.split('\t').toList
        val map = header.zip(cols).filter(_._2.nonEmpty).toMap
        val tconst = map.getOrElse("tconst", "")
        buf += PerfTitleBasics(
          tconst = tconst,
          titleType = map.getOrElse("titleType", ""),
          primaryTitle = map.getOrElse("primaryTitle", ""),
          originalTitle = map.getOrElse("originalTitle", ""),
          isAdult = map.get("isAdult").exists(_.toIntOption.contains(1)),
          startYear = map.get("startYear").flatMap(_.toIntOption).getOrElse(0),
          endYear = map.get("endYear").flatMap(_.toIntOption).getOrElse(0),
          runtimeMinutes = map.get("runtimeMinutes").flatMap(_.toIntOption).getOrElse(0),
          genres = map.get("genres").map(_.split(',').toList).getOrElse(Nil),
          _id = Id[PerfTitleBasics](tconst)
        )
        line = reader.readLine()
      }
      buf.toSeq
    } finally {
      reader.close()
    }
  }

  private def synthetic(limit: Int): (Seq[PerfTitleAka], Seq[PerfTitleBasics]) = {
    val rnd = ThreadLocalRandom.current()
    val akas = (0 until limit).map { i =>
      val id = f"tt$i%08d"
      PerfTitleAka(
        titleId = id,
        basicsId = Id[PerfTitleBasics](id),
        ordering = i,
        title = s"title-$i",
        region = if (i % 3 == 0) Some("US") else None,
        language = if (i % 4 == 0) Some("en") else None,
        types = if (i % 5 == 0) List("imdbDisplay") else Nil,
        attributes = Nil,
        isOriginalTitle = Some(true),
        _id = Id[PerfTitleAka](s"$id-$i")
      )
    }
    val basics = (0 until limit).map { i =>
      val id = f"tt$i%08d"
      PerfTitleBasics(
        tconst = id,
        titleType = if (i % 2 == 0) "movie" else "tvSeries",
        primaryTitle = s"primary-$i",
        originalTitle = s"original-$i",
        isAdult = false,
        startYear = 1980 + (i % 45),
        endYear = 0,
        runtimeMinutes = 60 + (i % 120),
        genres = if (i % 3 == 0) List("Drama") else List("Comedy"),
        _id = Id[PerfTitleBasics](id)
      )
    }
    (akas, basics)
  }

  // ---- DB + models (indexed for traversal/planning) ----
  class DB(tempDir: Path) extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override lazy val directory: Option[Path] = Some(tempDir)
    override def upgrades: List[DatabaseUpgrade] = Nil

    val aka: Collection[PerfTitleAka, PerfTitleAka.type] = store(PerfTitleAka, name = Some("aka"))
    val basics: Collection[PerfTitleBasics, PerfTitleBasics.type] = store(PerfTitleBasics, name = Some("basics"))
    val links: Collection[PerfTitleLink, PerfTitleLink.type] = store(PerfTitleLink, name = Some("links"))
  }
}

case class PerfTitleAka(titleId: String,
                        basicsId: Id[PerfTitleBasics],
                        ordering: Int,
                        title: String,
                        region: Option[String],
                        language: Option[String],
                        types: List[String],
                        attributes: List[String],
                        isOriginalTitle: Option[Boolean],
                        created: Timestamp = Timestamp(),
                        modified: Timestamp = Timestamp(),
                        _id: Id[PerfTitleAka] = Id[PerfTitleAka]()) extends lightdb.doc.RecordDocument[PerfTitleAka]

object PerfTitleAka extends lightdb.doc.RecordDocumentModel[PerfTitleAka] with JsonConversion[PerfTitleAka] {
  implicit val rw: RW[PerfTitleAka] = RW.gen
  val titleId: I[String] = field.index("titleId", _.titleId)
  val basicsId: I[Id[PerfTitleBasics]] = field.index("basicsId", _.basicsId)
  val ordering: I[Int] = field.index("ordering", _.ordering)
  val title: I[String] = field.index("title", _.title)
  val region: I[Option[String]] = field.index("region", _.region)
  val language: I[Option[String]] = field.index("language", _.language)
  val types: I[List[String]] = field.index("types", _.types)
  val attributes: F[List[String]] = field("attributes", _.attributes)
  val isOriginalTitle: I[Option[Boolean]] = field.index("isOriginalTitle", _.isOriginalTitle)

  val regionFacet: FF = field.facet("regionFacet", doc => doc.region.toList.map(r => FacetValue(r)), FacetConfig(multiValued = false))
  val languageFacet: FF = field.facet("languageFacet", doc => doc.language.toList.map(l => FacetValue(l)), FacetConfig(multiValued = false))
}

case class PerfTitleBasics(tconst: String,
                           titleType: String,
                           primaryTitle: String,
                           originalTitle: String,
                           isAdult: Boolean,
                           startYear: Int,
                           endYear: Int,
                           runtimeMinutes: Int,
                           genres: List[String],
                           created: Timestamp = Timestamp(),
                           modified: Timestamp = Timestamp(),
                           _id: Id[PerfTitleBasics] = Id[PerfTitleBasics]()) extends lightdb.doc.RecordDocument[PerfTitleBasics]

object PerfTitleBasics extends lightdb.doc.RecordDocumentModel[PerfTitleBasics] with JsonConversion[PerfTitleBasics] {
  implicit val rw: RW[PerfTitleBasics] = RW.gen
  val tconst: I[String] = field.index("tconst", _.tconst)
  val titleType: I[String] = field.index("titleType", _.titleType)
  val primaryTitle: F[String] = field("primaryTitle", _.primaryTitle)
  val originalTitle: F[String] = field("originalTitle", _.originalTitle)
  val isAdult: F[Boolean] = field("isAdult", _.isAdult)
  val startYear: I[Int] = field.index("startYear", _.startYear)
  val endYear: F[Int] = field("endYear", _.endYear)
  val runtimeMinutes: I[Int] = field.index("runtimeMinutes", _.runtimeMinutes)
  val genres: I[List[String]] = field.index("genres", _.genres)
}

case class PerfTitleLink(fromId: Id[PerfTitleBasics],
                         toId: Id[PerfTitleBasics],
                         kind: String,
                         created: Timestamp = Timestamp(),
                         modified: Timestamp = Timestamp(),
                         _id: Id[PerfTitleLink] = Id[PerfTitleLink]()) extends lightdb.doc.RecordDocument[PerfTitleLink]

object PerfTitleLink extends lightdb.doc.RecordDocumentModel[PerfTitleLink] with JsonConversion[PerfTitleLink] {
  implicit val rw: RW[PerfTitleLink] = RW.gen
  val fromId: I[Id[PerfTitleBasics]] = field.index("fromId", _.fromId)
  val toId: I[Id[PerfTitleBasics]] = field.index("toId", _.toId)
  val kind: I[String] = field.index("kind", _.kind)
}


