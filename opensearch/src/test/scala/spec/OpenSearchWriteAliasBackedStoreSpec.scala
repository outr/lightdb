package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchWriteAliasBackedStoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class WriteAliasDoc(value: String, _id: Id[WriteAliasDoc] = WriteAliasDoc.id()) extends Document[WriteAliasDoc]
  object WriteAliasDoc extends DocumentModel[WriteAliasDoc] with JsonConversion[WriteAliasDoc] {
    override implicit val rw: RW[WriteAliasDoc] = RW.gen
    val value: I[String] = field.index(_.value)
  }

  class DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    override def name: String = "OpenSearchWriteAliasBackedStoreSpec"

    val docs: lightdb.store.Collection[WriteAliasDoc, WriteAliasDoc.type] =
      store[WriteAliasDoc, WriteAliasDoc.type](WriteAliasDoc)
  }

  "OpenSearch write-alias backed store" should {
    "write via <alias>_write and read via <alias>" in {
      val prevUseIndexAlias = sys.props.get("lightdb.opensearch.useIndexAlias")
      val prevUseWriteAlias = sys.props.get("lightdb.opensearch.useWriteAlias")
      sys.props.put("lightdb.opensearch.useIndexAlias", "true")
      sys.props.put("lightdb.opensearch.useWriteAlias", "true")

      val db = new DB
      val test = (for {
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.upsert(WriteAliasDoc("one", Id[WriteAliasDoc]("one")))).next(tx.commit)
        }
        v <- db.docs.transaction(_.get(Id[WriteAliasDoc]("one"))).map(_.map(_.value))
        _ <- db.dispose
      } yield {
        v should be(Some("one"))
      }).guarantee(Task {
        prevUseIndexAlias match {
          case Some(v) => sys.props.put("lightdb.opensearch.useIndexAlias", v)
          case None => sys.props.remove("lightdb.opensearch.useIndexAlias")
        }
        prevUseWriteAlias match {
          case Some(v) => sys.props.put("lightdb.opensearch.useWriteAlias", v)
          case None => sys.props.remove("lightdb.opensearch.useWriteAlias")
        }
      })

      test
    }
  }
}



