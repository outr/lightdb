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
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchBulkConcurrencySpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Doc(name: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]

  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index("name", _.name)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchBulkConcurrencySpec"
    override lazy val directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs = store[Doc, Doc.type](Doc)
  }

  "OpenSearch bulk commits" should {
    "support bounded parallel bulk chunk submission" in {
      val keyMaxDocs = "lightdb.opensearch.bulk.maxDocs"
      val keyConcurrency = "lightdb.opensearch.bulk.concurrency"
      val keyRefresh = "lightdb.opensearch.refreshPolicy"

      val prev = Map(
        keyMaxDocs -> Profig(keyMaxDocs).opt[String],
        keyConcurrency -> Profig(keyConcurrency).opt[String],
        keyRefresh -> Profig(keyRefresh).opt[String]
      )

      Profig(keyMaxDocs).store("1")          // force many chunks
      Profig(keyConcurrency).store("4")      // allow concurrency > 1
      Profig(keyRefresh).store("true")       // keep test deterministic

      def restoreProps(): Unit = prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }

      val db = new DB
      val data = (1 to 25).toList.map(i => Doc(name = s"doc-$i", _id = Id(s"id-$i")))

      db.init.next {
        // Indexing and counting must be in separate transactions so the bulk buffer is committed before counting.
        db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(data)).unit
        }.next {
          db.docs.transaction { tx =>
            tx.count.map(_ should be(25))
          }
        }.guarantee(db.dispose).guarantee(Task(restoreProps()))
      }
    }
  }
}


