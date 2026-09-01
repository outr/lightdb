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
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * [[View.maintainScopedWithRemovals]]: a DELETE recomputes only its own scope.
 *
 * [[View.maintainScopedOn]] cannot place a removal - a delete carries no document - so it rebuilds the
 * whole view on every one. Where the id encodes its scope that is avoidable, and the difference is not
 * academic: in wb, marking one episode unwatched deletes a WatchState and rebuilt every profile's watch
 * summary, measured at ~10s on production.
 *
 * Correctness alone cannot tell the two apart (a full rebuild is also correct), so this records which
 * scopes the scoped relation is asked for. A full rebuild uses the unscoped relation and asks for none.
 */
@EmbeddedTest
class SQLiteScopedViewRemovalSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  case class Watch(profile: String, title: String, status: String, _id: Id[Watch]) extends Document[Watch]
  object Watch extends DocumentModel[Watch] with JsonConversion[Watch] {
    override implicit val rw: RW[Watch] = RW.gen
    val profile: I[String] = field.index(_.profile)
    val title: I[String] = field.index(_.title)
    val status: I[String] = field.index(_.status)
    /** Scope-encoding id, exactly like wb's WatchState: "$profile/$title". */
    def keyOf(profile: String, title: String): Id[Watch] = Id[Watch](s"$profile/$title")
    def apply(profile: String, title: String, status: String): Watch =
      Watch(profile, title, status, keyOf(profile, title))
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

  /** Every scope the view asked to recompute, in order. */
  private val asked = new ConcurrentLinkedQueue[String]()

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

    def scopedRelation(profile: String): Relation = { asked.add(profile); arm(Some(profile)) }

    /** The scope of a removed id, read back out of the id itself. */
    def scopeOfId(id: Id[Watch]): Option[String] = id.value.split('/').headOption.filter(_.nonEmpty)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def rows: rapid.Task[Set[(String, String, String)]] =
    DB.summaries.transaction(_.query.toList.map(_.map(s => (s.profile, s.title, s.status)).toSet))

  "SQLiteScopedViewRemovalSpec" should {
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
    "backfill and install removal-aware scope maintenance" in {
      DB.view.reBuild.map { count =>
        DB.view.maintainScopedWithRemovals(DB.watches, Summary.profile)(_.profile)(DB.scopeOfId)(DB.scopedRelation)
        count should be(3)
      }
    }
    "recompute ONLY the deleted row's scope, not the whole view" in {
      asked.clear()
      DB.watches.transaction(_.delete(Watch.keyOf("bob", "Heat"))).flatMap { _ =>
        rows.map { r =>
          // Correct: bob's row is gone, alice's are untouched.
          r should be(Set(("alice", "Dune", "watching"), ("alice", "Arrival", "watched")))
          // And it got there incrementally: only bob's scope was recomputed. A full rebuild would
          // have asked for no scope at all, and recomputing alice would mean the delete was not placed.
          scala.jdk.CollectionConverters.CollectionHasAsScala(asked).asScala.toSet should be(Set("bob"))
        }
      }
    }
    "still place a delete for the remaining profile" in {
      asked.clear()
      DB.watches.transaction(_.delete(Watch.keyOf("alice", "Dune"))).flatMap { _ =>
        rows.map { r =>
          r should be(Set(("alice", "Arrival", "watched")))
          scala.jdk.CollectionConverters.CollectionHasAsScala(asked).asScala.toSet should be(Set("alice"))
        }
      }
    }
    "dispose" in {
      DB.dispose.map(_ => succeed)
    }
  }
}
