package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.postgresql.{PostgreSQLStore, PostgreSQLStoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/**
 * Verifies `reIndex` streams read -> upsert with bounded memory on PostgreSQL (the at-scale target):
 * a computed indexed field whose derivation changes is stale until `reIndex`, then repopulated for all
 * pre-existing rows. Uses enough rows to exceed the streaming fetch size and force write flushes while
 * the read cursor is open.
 */
@EmbeddedTest
class PostgreSQLReIndexSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with PostgreSQLAvailability {
  // Exceeds both the read fetch size (1000) and several write-flush overflow cycles
  // (MaxInsertBatch 5000), so the run streams server-side with bounded memory rather than buffering.
  private val count = 12000

  "PostgreSQL reIndex" should {
    "re-derive a computed indexed field for all existing rows" in {
      ReIndexState.suffix = "v1"
      val docs = Person("bob", Id[Person]("bob")) :: (1 until count).map(i => Person(s"p$i", Id[Person](s"p$i"))).toList
      for {
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        v1Before <- DB.people.transaction(_.query.filter(_.tag === "bob-v1").toList.map(_.map(_.name)))
        _ = { ReIndexState.suffix = "v2" }
        staleV2 <- DB.people.transaction(_.query.filter(_.tag === "bob-v2").toList.map(_.map(_.name)))
        reIndexed <- DB.people.reIndex()
        countAfter <- DB.people.transaction(_.count)
        afterV2 <- DB.people.transaction(_.query.filter(_.tag === "bob-v2").toList.map(_.map(_.name)))
        afterV1 <- DB.people.transaction(_.query.filter(_.tag === "bob-v1").toList.map(_.map(_.name)))
        _ <- DB.truncate()
        _ <- DB.dispose
      } yield {
        v1Before should be(List("bob"))
        staleV2 should be(Nil)
        reIndexed should be(true)
        countAfter should be(count)
        afterV2 should be(List("bob"))
        afterV1 should be(Nil)
      }
    }
  }

  object ReIndexState {
    @volatile var suffix: String = "v1"
  }

  case class Person(name: String, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = field.index("name", _.name)
    val tag: I[String] = field.index("tag", d => s"${d.name}-${ReIndexState.suffix}")
  }

  object DB extends LightDB {
    override type SM = PostgreSQLStoreManager
    override val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
    override def name: String = "PostgreSQLReIndexSpec"
    override lazy val directory: Option[Path] = None
    val people: PostgreSQLStore[Person, Person.type] = store(Person)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
