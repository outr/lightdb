package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.field.Field.*
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

@EmbeddedTest
class LuceneDistinctSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Doc(groupId: String,
                 weight: Int,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen

    val groupId: I[String] = field.index(_.groupId)
    val weight: I[Int] = field.index(_.weight)

    override def map2Doc(map: Map[String, Any]): Doc =
      throw new RuntimeException("map2Doc not used in LuceneDistinctSpec")
  }

  class DB extends LightDB {
    override type SM = LuceneStore.type
    override val storeManager: LuceneStore.type = LuceneStore
    override def name: String = "LuceneDistinctSpec"
    override lazy val directory: Option[java.nio.file.Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    // Scala 2.13 struggles to infer local Doc/Model types here; spell them out.
    val docs: lightdb.store.Collection[Doc, Doc.type] = store[Doc, Doc.type](Doc)
  }

  "Lucene distinct" should {
    "return unique values and respect filters" in {
      val db = new DB
      val records = List(
        Doc("g1", 1),
        Doc("g1", 5),
        Doc("g2", 2),
        Doc("g2", 7),
        Doc("g3", 3)
      )

      val test = for
        _ <- db.init
        _ <- db.docs.transaction(_.insert(records))
        all <- db.docs.transaction(_.query.distinct(_.groupId).toList)
        heavy <- db.docs.transaction(_.query.filter(_.weight >= 5).distinct(_.groupId).toList)
        heavyCount <- db.docs.transaction(_.query.filter(_.weight >= 5).distinct(_.groupId).count)
        _ <- db.dispose
      yield {
        all.toSet should be(Set("g1", "g2", "g3"))
        heavy.toSet should be(Set("g1", "g2"))
        heavyCount should be(2)
      }

      test
    }
  }
}

