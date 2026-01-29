package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * Coverage: rollback should still discard buffered writes when we have NOT exceeded the max buffer size.
 *
 * This protects the common-case transactional behavior, while still allowing early flushes (and imperfect rollback)
 * when the write buffer grows too large.
 */
@EmbeddedTest
class OpenSearchBufferedWritingRollbackSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(value: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = OpenSearchBufferedWritingRollbackSpec.this.getClass.getSimpleName.replace("$", "")
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[Doc, Doc.type] = store(Doc)
  }

  "OpenSearch BufferedWritingTransaction rollback" should {
    "discard buffered writes when rollback happens before any early flush" in {
      val db = new DB

      val doc = Doc(value = "x", _id = Id[Doc]("d1"))

      val test =
        (for
          _ <- db.init
          _ <- db.docs.transaction.withBufferedBatch(1_000_000) { tx =>
            tx.truncate
              .next(tx.insert(doc))
              .next(tx.rollback)
          }
          count <- db.docs.transaction(_.count)
          _ <- db.dispose
        yield {
          count shouldBe 0
        })

      test
    }
  }
}

