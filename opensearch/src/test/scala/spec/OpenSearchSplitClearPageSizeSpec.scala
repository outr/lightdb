package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * TDD regression test for the LogicalNetwork symptom:
 * - With SplitCollection (storage + OpenSearch), `clearLimit.clearPageSize.stream` must not default to 10 results.
 *
 * This is intentionally a SplitStore test (HashMapStore + OpenSearchStore) because LogicalNetwork uses SplitCollection.
 */
@EmbeddedTest
class OpenSearchSplitClearPageSizeSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(value: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  class DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager = SplitStoreManager(HashMapStore, OpenSearchStore)
    override def name: String = OpenSearchSplitClearPageSizeSpec.this.getClass.getSimpleName.replace("$", "")
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docsStore: lightdb.store.Collection[Doc, Doc.type] = store[Doc, Doc.type](Doc)
  }

  "OpenSearch SplitCollection clearPageSize streaming" should {
    "stream all docs (not default to 10) when limit and pageSize are cleared" in {
      val db = new DB
      val docsList = (1 to 25).toList.map(i => Doc(value = s"v$i", _id = Id[Doc](s"d$i")))

      val test = for {
        _ <- db.init
        _ <- db.docsStore.transaction { tx =>
          tx.truncate.next(tx.insert(docsList)).next(tx.commit)
        }
        ids <- db.docsStore.transaction { tx =>
          tx.query
            .clearLimit
            .clearPageSize
            .id
            .stream
            .map(_.value)
            .toList
        }
        _ <- db.dispose
      } yield {
        // This is the crux: OpenSearch defaults to 10 hits if `size` is omitted. That must NOT happen for streaming.
        ids.length shouldBe 25
      }

      test
    }
  }
}

