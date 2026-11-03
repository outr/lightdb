package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A field-scoped write (`updateFields`) on a split collection never writes storage: it rewrites the documents' search
 * index rows from what storage holds when it runs. A document written after the caller read it keeps that write, and
 * the index answers from it; a document removed since stays removed.
 */
@EmbeddedTest
class RocksDBAndLuceneFieldReIndexSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {

  case class Person(name: String, city: String, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val city: I[String] = field.index("city", (d: Person) => d.city)
    val nameLength: I[Int] = field.index("nameLength", (d: Person) => d.name.length)
  }

  private val specName = getClass.getSimpleName
  private val dbPath: Path = Path.of(s"db/$specName")
  if Files.exists(dbPath) then Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = SplitStoreManager(RocksDBStore, LuceneStore)
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(dbPath)
    val people: Collection[Person, Person.type] = store(Person)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "updateFields on a split collection" should {
    "initialize the database" in DB.init.succeed
    "insert the documents" in {
      DB.people.transaction(_.insert(List(Person("Alice", "Denver", Person.id("a")), Person("Cy", "Reno", Person.id("c")),
        Person("Dee", "Ames", Person.id("d"))))).succeed
    }
    "keep a write made after the caller read the document, in storage and in the index" in {
      for {
        stale <- DB.people.transaction(tx => tx.get(Person.id("c")))
        _ <- DB.people.transaction(_.upsert(stale.get.copy(city = "Tahoe")))
        written <- DB.people.transaction(_.updateFields(stale.toList, List(Person.nameLength)))
        stored <- DB.people.transaction(_.get(Person.id("c")))
        byCity <- DB.people.transaction(_.query.filter(_.city === "Tahoe").toList)
        byOldCity <- DB.people.transaction(_.query.filter(_.city === "Reno").toList)
        byLength <- DB.people.transaction(_.query.filter(_.nameLength === 2).toList)
      } yield {
        written shouldBe 1
        stored.map(_.city) shouldBe Some("Tahoe")
        byCity.map(_._id.value) shouldBe List("c")
        byOldCity shouldBe Nil
        byLength.map(_._id.value) shouldBe List("c")
      }
    }
    "leave a document removed since the caller read it removed" in {
      for {
        stale <- DB.people.transaction(tx => tx.get(Person.id("d")))
        _ <- DB.people.transaction(_.delete(Person.id("d")))
        written <- DB.people.transaction(_.updateFields(stale.toList, List(Person.nameLength)))
        stored <- DB.people.transaction(_.get(Person.id("d")))
        found <- DB.people.transaction(_.query.filter(_.city === "Ames").toList)
      } yield {
        written shouldBe 0
        stored shouldBe None
        found shouldBe Nil
      }
    }
    "dispose the database" in DB.dispose.succeed
  }
}
