package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchMetrics}
import lightdb.opensearch.OpenSearchStore
import lightdb.store.BufferedWritingTransaction
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

/**
 * Regression/spec coverage for OpenSearchTransaction using BufferedWritingTransaction:
 * - When the buffered write map exceeds MaxTransactionWriteBuffer, we should flush before commit to avoid OOM.
 *
 * This spec uses OpenSearchMetrics as an observable side-effect that a bulk flush occurred before commit.
 */
@EmbeddedTest
class OpenSearchBufferedWritingFlushSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(value: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = OpenSearchBufferedWritingFlushSpec.this.getClass.getSimpleName.replace("$", "")
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[Doc, Doc.type] = store(Doc)
  }

  "OpenSearch BufferedWritingTransaction flush" should {
    "flush before commit when max buffer is exceeded" in {
      val db = new DB

      val originalMax = BufferedWritingTransaction.MaxTransactionWriteBuffer
      BufferedWritingTransaction.MaxTransactionWriteBuffer = 5

      // Ensure metrics are enabled so we can assert a flush occurred.
      Profig("lightdb.opensearch.metrics.enabled").store(true)

      val baseUrlKey = Profig("lightdb.opensearch.baseUrl").as[String].stripSuffix("/")

      val docs = (1 to 25).toList.map(i => Doc(value = s"v$i", _id = Id[Doc](s"d$i")))

      val test = db.init.next {
        db.docs.transaction { tx =>
          val before = Task(OpenSearchMetrics.snapshot(baseUrlKey))
          val insertAll = tx.truncate.next(tx.insert(docs))
          val after = Task(OpenSearchMetrics.snapshot(baseUrlKey))

          for {
            b <- before
            _ <- insertAll
            a <- after
          } yield {
            // If we flushed before commit, bulkDocs should have increased within the transaction body.
            (a.bulkDocs - b.bulkDocs) should be > 0L
          }
        }.guarantee(db.dispose)
      }.guarantee(Task {
        BufferedWritingTransaction.MaxTransactionWriteBuffer = originalMax
      })

      test
    }
  }
}

