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
import rapid.{AsyncTaskSpec, Task}

/**
 * A transaction that fails must roll back its own writes and leave the
 * store usable. `IndexWriter.rollback()` closes the writer, so without a
 * reopen every later write failed with "this IndexWriter is closed".
 */
@EmbeddedTest
class LuceneRollbackSurvivalSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen

    val name: I[String] = field.index(_.name)

    override def map2Doc(map: Map[String, Any]): Doc =
      throw new RuntimeException("map2Doc not used in LuceneRollbackSurvivalSpec")
  }

  class DB extends LightDB {
    override type SM = LuceneStore.type
    override val storeManager: LuceneStore.type = LuceneStore
    override def name: String = "LuceneRollbackSurvivalSpec"
    override lazy val directory: Option[java.nio.file.Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: lightdb.store.Collection[Doc, Doc.type] = store[Doc, Doc.type](Doc)()
  }

  "a failed Lucene transaction" should {
    "roll back its writes and leave the store writable and searchable" in {
      val db = new DB
      val test = for
        _ <- db.init
        _ <- db.docs.transaction(_.insert(Doc("kept")))
        failed <- db.docs.transaction { tx =>
          tx.insert(Doc("discarded")).flatMap(_ => Task.error[Unit](new RuntimeException("boom")))
        }.attempt
        _ <- db.docs.transaction(_.insert(Doc("after")))
        names <- db.docs.transaction(_.query.toList.map(_.map(_.name).toSet))
        found <- db.docs.transaction(_.query.filter(_.name === "after").toList)
        _ <- db.dispose
      yield {
        failed.isFailure should be(true)
        names should be(Set("kept", "after"))
        found.map(_.name) should be(List("after"))
      }
      test
    }
  }
}
