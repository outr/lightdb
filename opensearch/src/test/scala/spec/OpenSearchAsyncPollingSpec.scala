package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import scala.concurrent.duration.DurationInt

@EmbeddedTest
class OpenSearchAsyncPollingSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = OpenSearchStore
    override def name: String = getClass.getSimpleName
    override def directory = None
    override def upgrades = Nil

    val docs: Collection[Doc, Doc.type] = store(Doc)
  }

  "OpenSearch async polling" should {
    "complete deleteByQuery when the sync request times out" in {
      val db = new DB
      val originalThreshold = OpenSearchStore.asyncTaskPollAfter
      OpenSearchStore.asyncTaskPollAfter = 1.millis

      val client = OpenSearchClient(OpenSearchConfig.from(db, db.docs.name))
      val indexName = db.docs.asInstanceOf[OpenSearchStore[Doc, Doc.type]].readIndexName
      val deleteQuery = obj("query" -> obj("match_all" -> obj()))

      val test =
        db.init.next {
          db.docs.transaction { tx =>
            tx.insert(List(Doc("alpha"), Doc("bravo"), Doc("charlie"))).map(_ => ())
          }
        }.flatMap { _ =>
          client.deleteByQuery(indexName, deleteQuery, refresh = Some("true"))
        }.flatMap { deleted =>
          db.docs.transaction(_.count).map { remaining =>
            deleted should be(3)
            remaining should be(0)
          }
        }

      test.guarantee(db.dispose).guarantee(Task {
        OpenSearchStore.asyncTaskPollAfter = originalThreshold
      })
    }
  }
}
