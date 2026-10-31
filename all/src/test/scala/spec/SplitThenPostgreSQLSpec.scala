package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A model first opened by a split RocksDB + Lucene store (whose halves initialize at the same time) can then be opened
 * by a PostgreSQL store in the same JVM: each field the model does not declare is registered once, so the table
 * PostgreSQL creates names each column once.
 */
@EmbeddedTest
class SplitThenPostgreSQLSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with PostgreSQLAvailability {
  import SplitThenPostgreSQLSpec.*

  private lazy val root = Path.of(s"db/${getClass.getSimpleName}")

  private lazy val split: DB = {
    if Files.exists(root) then Files.walk(root).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
    new DB("split", Some(root), SplitStoreManager(RocksDBStore, LuceneStore))
  }

  private lazy val postgres: DB = new DB("postgres", None, PostgreSQLTestSupport.storeManager)

  private val entry = Entry("first", Some("topic-1"), 3, Id[Entry]("entry-1"))

  "A model opened by a split store" should {
    "register each undeclared field once" in {
      split.init.map { _ =>
        val names = Entry.fields.map(_.name)
        names.diff(names.distinct) shouldBe Nil
        names should contain allOf ("label", "topicId", "count")
      }
    }
    "write and read through the split store" in {
      for {
        _ <- split.entries.transaction(_.upsert(entry))
        got <- split.entries.transaction(_.get(entry._id))
      } yield got shouldBe Some(entry)
    }
    "then open a PostgreSQL store for the same model" in {
      for {
        _ <- postgres.init
        _ <- postgres.entries.transaction(_.truncate)
        _ <- postgres.entries.transaction(_.upsert(entry))
        got <- postgres.entries.transaction(_.get(entry._id))
      } yield got shouldBe Some(entry)
    }
    "dispose both" in {
      for {
        _ <- postgres.entries.transaction(_.truncate)
        _ <- postgres.dispose
        _ <- split.dispose
      } yield succeed
    }
  }
}

object SplitThenPostgreSQLSpec {
  case class Entry(label: String, topicId: Option[String], count: Int, _id: Id[Entry]) extends Document[Entry]

  /** Declares only `_id`; `label`, `topicId` and `count` are registered when a store initializes. */
  object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen
  }

  class DB(suffix: String, dir: Option[Path], sm: StoreManager) extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = sm

    override def name: String = s"SplitThenPostgreSQLSpec-$suffix"

    lazy val directory: Option[Path] = dir

    override protected def disposeOnShutdown: Boolean = false

    val entries: Store[Entry, Entry.type] = store(Entry).withName("split_then_pg_entries")()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
