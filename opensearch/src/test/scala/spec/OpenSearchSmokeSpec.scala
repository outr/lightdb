package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field.*
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import scala.concurrent.duration.DurationInt

/**
 * Minimal smoke test to ensure:
 * - Testcontainers can start OpenSearch
 * - LightDB can create an index
 * - bulk upsert works
 * - count + truncate work
 *
 * Full parity is covered by the abstract specs once enabled.
 */
@EmbeddedTest
class OpenSearchSmokeSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport { spec =>
  case class TestDoc(value: Int, _id: Id[TestDoc] = Id[TestDoc]()) extends Document[TestDoc]

  object TestDoc extends DocumentModel[TestDoc] with JsonConversion[TestDoc] {
    override implicit val rw: RW[TestDoc] = RW.gen

    val value: I[Int] = field.index("value", _.value)
  }

  object db extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchSmokeSpec"
    override lazy val directory: Option[java.nio.file.Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs = store[TestDoc, TestDoc.type](TestDoc)
    scribe.info("OpenSearchSmokeSpec: store created")
  }

  "OpenSearch (smoke)" should {
    "initialize" in {
      db.init.succeed
    }
    "truncate" in {
      db.docs.transaction(_.truncate).succeed
    }
    "insert a record" in {
      db.docs.transaction(_.upsert(TestDoc(1, _id = Id("one")))).succeed
    }
    "lookup the inserted record" in {
      db.docs.transaction(_.get(Id[TestDoc]("one"))).map { found =>
        found.map(_.value) should be(Some(1))
      }
    }
    "count the records" in {
      Task.condition(db.docs.transaction(_.count).map(_ == 1), timeout = 10.seconds).next {
        db.docs.transaction(_.count).map { count =>
          count should be(1)
        }
      }
    }
    "truncate again" in {
      db.docs.transaction(_.truncate).map { truncated =>
        truncated should be(1)
      }
    }
    "dispose" in {
      db.dispose.succeed
    }
  }
}


