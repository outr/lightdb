package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * A transaction that only read has nothing to commit or roll back: its commit runs no Lucene commit, and its abort
 * leaves the writes other transactions have pending in the shared writer alone.
 */
@EmbeddedTest
class LuceneReadOnlyTransactionSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Doc(name: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Doc.id()) extends RecordDocument[Doc]

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB extends LightDB {
    override type SM = LuceneStore.type
    override val storeManager: LuceneStore.type = LuceneStore
    override def name: String = "LuceneReadOnlyTransactionSpec"
    override lazy val directory: Option[java.nio.file.Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: LuceneStore[Doc, Doc.type] = store[Doc, Doc.type](Doc)()
  }

  private val db = new DB
  private def commits: Long = db.docs.index.commitCount

  "Lucene transactions" should {
    "initialize" in db.init.succeed
    "run one commit for a transaction that wrote" in {
      val before = commits
      db.docs.transaction(_.insert(List(Doc("alpha"), Doc("beta")))).map { _ =>
        commits shouldBe before + 1
      }
    }
    "run no commit for a transaction that only read" in {
      val before = commits
      db.docs.transaction { tx =>
        for
          all <- tx.query.toList
          beta <- tx.query.filter(_.name === "beta").toList
          count <- tx.count
          one <- tx.get(all.head._id)
        yield (all.size, beta.size, count, one.nonEmpty)
      }.map { result =>
        result shouldBe ((2, 1, 2, true))
        commits shouldBe before
      }
    }
    "leave another transaction's pending writes alone when a read-only transaction aborts" in {
      for
        writer <- db.docs.transaction.create()
        _ <- writer.insert(Doc("gamma"))
        failed <- db.docs.transaction { tx =>
          tx.count.flatMap(_ => Task.error[Int](new RuntimeException("abort")))
        }.attempt
        _ <- db.docs.transaction.release(writer)
        names <- db.docs.transaction(_.query.toList.map(_.map(_.name).toSet))
      yield {
        failed.isFailure shouldBe true
        names shouldBe Set("alpha", "beta", "gamma")
      }
    }
    "roll back a transaction that wrote" in {
      for
        failed <- db.docs.transaction { tx =>
          tx.insert(Doc("discarded")).flatMap(_ => Task.error[Unit](new RuntimeException("abort")))
        }.attempt
        names <- db.docs.transaction(_.query.toList.map(_.map(_.name).toSet))
      yield {
        failed.isFailure shouldBe true
        names shouldBe Set("alpha", "beta", "gamma")
      }
    }
    "dispose" in db.dispose.succeed
  }
}
