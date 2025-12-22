package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.sql.{SQLCollectionManager, SQLiteStore}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.Sort
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * Validates that SQLite-specific optimizations (FTS + multi-value indexes) are functional.
 *
 * Note: This does not attempt to validate performance, only correctness + that translation doesn't throw.
 */
@EmbeddedTest
class SQLiteFTSSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  final class TestDB extends LightDB {
    override type SM = SQLCollectionManager
    override val storeManager: SQLCollectionManager = SQLiteStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: S[Doc, Doc.type] = store(Doc)
  }

  private def withDB[A](f: TestDB => Task[A]): Task[A] = {
    val db = new TestDB
    db.init.next(f(db)).guarantee(db.dispose)
  }

  "SQLite FTS" should {
    "support tokenized search via FTS (no throw)" in {
      withDB { TestDB =>
        for {
          _ <- TestDB.docs.transaction { txn =>
          txn.insert(List(
            Doc(name = "Adam", _id = Doc.id("adam")),
            Doc(name = "Brenda", _id = Doc.id("brenda"))
          )).unit
          }
          total <- TestDB.docs.transaction { txn =>
          txn.query
            .filter(_.search === "adam")
            .countTotal(true)
            .id
            .search
            .map(_.total.getOrElse(0))
          }
        } yield total should be >= 1
      }
    }

    "support BestMatch ranking (bm25) and non-constant scores" in {
      withDB { TestDB =>
        for {
          _ <- TestDB.docs.transaction { txn =>
          txn.truncate.unit.next {
            txn.insert(List(
              Doc(name = "adam", _id = Doc.id("a1")),
              Doc(name = "adam adam adam", _id = Doc.id("a3"))
            )).unit
          }
          }
          scored <- TestDB.docs.transaction { txn =>
          txn.query
            .filter(_.search === "adam")
            .clearSort
            .sort(Sort.BestMatch())
            .scored
            .clearPageSize
            .streamScoredPage
            .toList
          }
        } yield {
          scored.map(_._1.name).toSet should be(Set("adam", "adam adam adam"))
          scored.map(_._2).distinct.size should be >= 2
        }
      }
    }
  }

  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc] {
    lazy val searchText: String = name.toLowerCase
  }

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen

    val name = field.index(_.name)
    val search = field.tokenized("search", _.searchText)
  }
}


