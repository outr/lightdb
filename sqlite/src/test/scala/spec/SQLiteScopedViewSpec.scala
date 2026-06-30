package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.view.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * Scope-incremental view maintenance ([[View.maintainScopedOn]]): a profile-partitioned summary view
 * is kept current by recomputing only the changed profile's partition after each dependency commit,
 * not by rebuilding the whole view. Asserts the materialized results stay correct across inserts,
 * status upserts, an in-scope drop (a row filtered out by an upsert), and a delete (which falls back
 * to a full rebuild). Mirrors the shape of wb's WatchSummary movie-arm (composite `_id`, partitioned
 * by profile).
 */
@EmbeddedTest
class SQLiteScopedViewSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  case class Watch(profile: String, title: String, status: String, _id: Id[Watch] = Watch.id()) extends Document[Watch]
  object Watch extends DocumentModel[Watch] with JsonConversion[Watch] {
    override implicit val rw: RW[Watch] = RW.gen
    val profile: I[String] = field.index(_.profile)
    val title: I[String] = field.index(_.title)
    val status: I[String] = field.index(_.status)
  }

  case class Summary(profile: String, title: String, status: String, _id: Id[Summary] = Summary.id()) extends Document[Summary]
  object Summary extends DocumentModel[Summary] with JsonConversion[Summary] {
    override implicit val rw: RW[Summary] = RW.gen
    val profile: I[String] = field.index(_.profile)
    val title: I[String] = field.index(_.title)
    val status: I[String] = field.index(_.status)
  }

  private val specName = getClass.getSimpleName
  private val dbPath: Path = Path.of(s"db/$specName")
  if Files.exists(dbPath) then {
    Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = SQLiteStore
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(dbPath)

    val watches: Collection[Watch, Watch.type] = store(Watch)()
    val summaries: Collection[Summary, Summary.type] = store(Summary)()

    private def arm(extra: Option[String]): Relation = {
      val base = from(watches, "w").where(w => w(Watch.status) !== lit("hidden"))
      val filtered = extra match {
        case Some(p) => base.where(w => w(Watch.profile) === lit(p))
        case None => base
      }
      filtered.select(w => List(
        Summary._id := concat(w(Watch.profile), lit("/"), w(Watch.title)),
        Summary.profile := w(Watch.profile),
        Summary.title := w(Watch.title),
        Summary.status := w(Watch.status)
      ))
    }

    val view: View[Summary, Summary.type] = View(summaries, Materialization.cachedManual)(arm(None))

    def scopedRelation(profile: String): Relation = arm(Some(profile))

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def rows: rapid.Task[Set[(String, String, String)]] =
    DB.summaries.transaction(_.query.toList.map(_.map(s => (s.profile, s.title, s.status)).toSet))

  "SQLiteScopedViewSpec" should {
    "initialize and seed two profiles' watches" in {
      for {
        _ <- DB.init
        _ <- DB.watches.transaction(_.insert(List(
          Watch("alice", "Dune", "watching"),
          Watch("alice", "Arrival", "watched"),
          Watch("bob", "Heat", "watching")
        )))
      } yield succeed
    }
    "backfill the view and install scope-incremental maintenance" in {
      DB.view.reBuild.map { count =>
        DB.view.maintainScopedOn(DB.watches, Summary.profile)(_.profile)(DB.scopedRelation)
        count should be(3)
      }
    }
    "materialize the initial summary" in {
      rows.map(_ should be(Set(
        ("alice", "Dune", "watching"), ("alice", "Arrival", "watched"), ("bob", "Heat", "watching"))))
    }
    "incrementally add a row when a new watch commits (only that profile recomputed)" in {
      DB.watches.transaction(_.insert(Watch("alice", "Sicario", "watching"))).flatMap { _ =>
        rows.map(_ should be(Set(
          ("alice", "Dune", "watching"), ("alice", "Arrival", "watched"),
          ("alice", "Sicario", "watching"), ("bob", "Heat", "watching"))))
      }
    }
    "incrementally reflect a status change via upsert" in {
      DB.watches.transaction { tx =>
        tx.query.filter(w => w.profile === "alice" && w.title === "Dune").toList.flatMap { found =>
          tx.upsert(found.head.copy(status = "watched"))
        }
      }.flatMap { _ =>
        rows.map(_ should contain(("alice", "Dune", "watched")))
      }
    }
    "drop a row from its scope when an upsert filters it out (status -> hidden)" in {
      DB.watches.transaction { tx =>
        tx.query.filter(w => w.profile === "alice" && w.title === "Sicario").toList.flatMap { found =>
          tx.upsert(found.head.copy(status = "hidden"))
        }
      }.flatMap { _ =>
        rows.map { r =>
          r should not contain (("alice", "Sicario", "watching"))
          r.exists(t => t._1 == "alice" && t._2 == "Sicario") should be(false)
          r should contain(("bob", "Heat", "watching"))
        }
      }
    }
    "fall back to a full rebuild on delete (removed rows can't be scoped)" in {
      DB.watches.transaction { tx =>
        tx.query.filter(w => w.profile === "bob" && w.title === "Heat").toList.flatMap { found =>
          tx.delete(found.head._id)
        }
      }.flatMap { _ =>
        rows.map { r =>
          r.exists(_._1 == "bob") should be(false)
          r should be(Set(
            ("alice", "Dune", "watched"), ("alice", "Arrival", "watched")))
        }
      }
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }
}
