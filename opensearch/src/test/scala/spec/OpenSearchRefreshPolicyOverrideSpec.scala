package spec

import fabric.*
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field.*
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.opensearch.OpenSearchIndexName
import lightdb.opensearch.OpenSearchStore
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchRefreshPolicyOverrideSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]

  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index("name", _.name)
  }

  "OpenSearchTransaction.withRefreshPolicy" should {
    "override refresh behavior for a single transaction" in {
      val storeName = "Doc"
      val keyRefresh = s"lightdb.opensearch.$storeName.refreshPolicy"
      val prev = Profig(keyRefresh).opt[String]
      // Override the global test default (refresh=true) for this store only.
      Profig(keyRefresh).store("false")

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchRefreshPolicyOverrideSpec"
        override def directory = None
        override def upgrades: List[DatabaseUpgrade] = Nil

        val docs = store[Doc, Doc.type](Doc)
      }

      val db = new DB
      val cfg = OpenSearchConfig.from(db, storeName)
      val client = OpenSearchClient(cfg)
      val index = OpenSearchIndexName.default(db.name, storeName, cfg)

      def restoreProps(): Task[Unit] = Task {
        prev match {
          case Some(v) => Profig(keyRefresh).store(v)
          case None => Profig(keyRefresh).remove()
        }
      }

      val test = for
        _ <- client.deleteIndex(index)
        // Create index with refresh disabled so visibility is deterministic unless refresh=true is used.
        _ <- client.createIndex(index, obj(
          "settings" -> obj("index" -> obj("refresh_interval" -> str("-1"))),
          "mappings" -> obj("dynamic" -> bool(true))
        ))
        _ <- db.init

        // IMPORTANT:
        // Store.transaction(...) always commits in `guarantee(...)` even if the user task fails.
        // To test "early flush without commit refresh", we must explicitly rollback (no commit).
        _ <- db.docs.transaction.withBufferedBatch(1).create().flatMap { tx =>
          tx.truncate
            .next(tx.insert(Doc("no-refresh-a", _id = Id("a"))))
            .next(tx.insert(Doc("no-refresh-b", _id = Id("b")))) // triggers early flush (buffer > 1)
            .next(tx.rollback)
            .guarantee(db.docs.transaction.release(tx))
        }

        // With refresh_interval=-1 and no explicit refresh, doc should not be visible yet.
        before <- db.docs.transaction { tx =>
          tx.query
            .filter(m => Filter.Equals[Doc, Id[Doc]](m._id.name, Id[Doc]("a")))
            .countTotal(true)
            .search
            .flatMap(_.list)
        }

        _ <- db.docs.transaction.withBufferedBatch(1) { tx0 =>
          tx0 match {
            case tx: lightdb.opensearch.OpenSearchTransaction[Doc, Doc.type] =>
              tx.withRefreshPolicy(Some("true"))
                .insert(Doc("with-refresh-c", _id = Id("c")))
                .next(tx.insert(Doc("with-refresh-d", _id = Id("d")))) // triggers early flush (buffer > 1)
                .next(tx.rollback)
            case _ =>
              Task.error(new RuntimeException("Expected OpenSearchTransaction"))
          }
        }

        after <- db.docs.transaction { tx =>
          tx.query
            .filter(m => Filter.Equals[Doc, Id[Doc]](m._id.name, Id[Doc]("c")))
            .countTotal(true)
            .search
            .flatMap(_.list)
        }

        _ <- db.dispose
      yield {
        before shouldBe Nil
        after.map(_._id.value) shouldBe List("c")
      }

      test.guarantee(restoreProps())
    }
  }
}


