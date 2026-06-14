package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.h2.H2Store
import lightdb.id.Id
import lightdb.sql.SQLCollectionManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

/**
 * Verifies that `reIndex` on a full (StoreMode.All) collection re-derives computed/indexed field
 * values for existing rows by re-writing every document. The `tag` field's value depends on a mutable
 * suffix, simulating a newly-added or changed derivation: rows written before the change keep their
 * old derived value until `reIndex` rebuilds them.
 */
@EmbeddedTest
class H2ReIndexSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  // Large enough to exceed the streaming fetch size and force write-batch flushes while the read
  // cursor is still open on the same transaction/connection (the case reIndex must handle).
  private val count = 2500

  "reIndex" should {
    "re-derive a computed indexed field for existing rows (streaming, same transaction)" in {
      val db = new TestDB
      ReIndexState.suffix = "v1"
      val docs = Person("bob") :: (1 until count).map(i => Person(s"p$i")).toList
      for {
        _ <- db.init
        _ <- db.people.transaction(_.insert(docs))
        v1Before <- db.people.transaction(_.query.filter(_.tag === "bob-v1").toList.map(_.map(_.name)))
        _ = { ReIndexState.suffix = "v2" } // the derivation changes (e.g. a new/changed computed field)
        staleV2 <- db.people.transaction(_.query.filter(_.tag === "bob-v2").toList.map(_.map(_.name)))
        reIndexed <- db.people.reIndex()
        countAfter <- db.people.transaction(_.count)
        afterV2 <- db.people.transaction(_.query.filter(_.tag === "bob-v2").toList.map(_.map(_.name)))
        afterV1 <- db.people.transaction(_.query.filter(_.tag === "bob-v1").toList.map(_.map(_.name)))
        _ <- db.dispose
      } yield {
        v1Before should be(List("bob"))  // derived at insert with the v1 suffix
        staleV2 should be(Nil)           // stale until reIndex
        reIndexed should be(true)
        countAfter should be(count)      // streaming re-write preserves every document
        afterV2 should be(List("bob"))   // reIndex re-derived with the v2 suffix
        afterV1 should be(Nil)           // the old derived value is gone
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

  final class TestDB extends LightDB {
    override type SM = SQLCollectionManager
    override val storeManager: SQLCollectionManager = H2Store
    override def name: String = "H2ReIndexSpec"
    override def directory: Option[java.nio.file.Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    val people: S[Person, Person.type] = store(Person)()
  }
}
