package spec

import fabric._
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field._
import lightdb.id.Id
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.opensearch.{OpenSearchDeadLetterIndexName, OpenSearchIndexName, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchDeadLetterSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]

  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index("name", _.name)
  }

  "OpenSearch dead-letter capture" should {
    "capture bulk item failures into a dead-letter index (best-effort) and still fail the transaction" in {
      val storeName = "Doc"
      val keyEnabled = s"lightdb.opensearch.$storeName.deadLetter.enabled"
      val prevEnabled = Profig(keyEnabled).opt[Boolean]
      Profig(keyEnabled).store(true)

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchDeadLetterSpec"
        override def directory = None
        override def upgrades: List[DatabaseUpgrade] = Nil

        val docs = store[Doc, Doc.type](Doc)
      }

      val db = new DB

      val config = OpenSearchConfig.from(db, storeName)
      val client = OpenSearchClient(config)
      val strictIndex = OpenSearchIndexName.default(db.name, storeName, config)
      val deadIndex = OpenSearchDeadLetterIndexName.default(db.name, storeName, config)

      def restoreProps(): Task[Unit] = Task {
        prevEnabled match {
          case Some(v) => Profig(keyEnabled).store(v)
          case None => Profig(keyEnabled).remove()
        }
      }

      val test = for {
        _ <- client.deleteIndex(strictIndex)
        _ <- client.deleteIndex(deadIndex)
        // Create a mapping that will deterministically reject our inserts.
        //
        // When running against a developer's local OpenSearch cluster, there may be existing index templates that
        // add mappings for common fields (like `name`), which can make `dynamic=strict` alone non-deterministic.
        // For the purpose of this test we just need bulk item failures so the dead-letter path is exercised.
        //
        // By mapping `name` as a numeric field, indexing string values will reliably fail with a mapper exception.
        // Store init will not override existing indices.
        _ <- client.createIndex(strictIndex, obj(
          "mappings" -> obj(
            "dynamic" -> str("strict"),
            "properties" -> obj(
              "name" -> obj("type" -> str("long"))
            )
          )
        ))
        _ <- db.init
        // Attempt insert: should fail due to strict mapping
        result <- db.docs.transaction { tx =>
          // Do NOT truncate here: truncate may delete/recreate the index (faster path), which would remove the strict
          // mapping we intentionally created above.
          tx.insert(List(
            Doc("one", _id = Id("1")),
            Doc("two", _id = Id("2")),
            Doc("three", _id = Id("3"))
          )).next(tx.commit)
        }.attempt
        _ = result.isFailure should be(true)
        _ <- client.refreshIndex(deadIndex).attempt.unit
        c <- client.count(deadIndex, obj("query" -> obj("match_all" -> obj())))
        _ <- db.dispose
      } yield {
        c shouldBe 3
      }

      test.guarantee(restoreProps())
    }
  }
}


