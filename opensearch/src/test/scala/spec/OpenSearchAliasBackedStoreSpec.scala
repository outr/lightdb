package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchAliasBackedStoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class AliasDoc(value: String, _id: Id[AliasDoc] = AliasDoc.id()) extends Document[AliasDoc]
  object AliasDoc extends DocumentModel[AliasDoc] with JsonConversion[AliasDoc] {
    override implicit val rw: RW[AliasDoc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  class DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = OpenSearchStore
    override def directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    override def name: String = "OpenSearchAliasBackedStoreSpec"

    val docs: lightdb.store.Collection[AliasDoc, AliasDoc.type] = store[AliasDoc, AliasDoc.type](AliasDoc)
  }

  "OpenSearch alias-backed store" should {
    "initialize and read/write via alias" in {
      val key = "lightdb.opensearch.useIndexAlias"
      val previous = Profig(key).opt[String]
      Profig(key).store("true")

      val db = new DB
      val test = (for
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.upsert(AliasDoc("one", Id[AliasDoc]("one")))).next(tx.commit)
        }
        v <- db.docs.transaction(_.get(Id[AliasDoc]("one"))).map(_.map(_.value))
        _ <- db.dispose
      yield {
        v should be(Some("one"))
      }).guarantee(Task {
        previous match {
          case Some(v) => Profig(key).store(v)
          case None => Profig(key).remove()
        }
      })

      test
    }
  }
}


