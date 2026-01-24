package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalFastTotalFromPersistedPostingsSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Force persisted seeding to decline materialization for large candidate sets, so we exercise postings-count total.
    System.setProperty("lightdb.traversal.persistedIndex.maxSeedSize", "1")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally System.clearProperty("lightdb.traversal.persistedIndex.maxSeedSize")
  }

  private lazy val specName: String = getClass.getSimpleName
  override def traversalStoreManager: TraversalManager = super.traversalStoreManager

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: S[Person, Person.type] = store(Person)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: F[String] = field("name", _.name)
    val age: I[Int] = field.index(_.age)
  }

  specName should {
    "compute total from persisted postings count when seed materialization is disabled" in {
      val docs = (1 to 20).toList.map(i => Person(name = s"p$i", age = if i <= 12 then 1 else 2, _id = Id(s"p$i")))
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.insert(docs))
        // Ensure persisted index is ready so query engine can use it.
        _ <- DB.people.buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(1)
            .countTotal(true)
            .sort(Sort.IndexOrder)
            .filter(_.age === 1)
            .search
        }
        list <- results.stream.toList
      yield {
        results.total shouldBe Some(12)
        list.size shouldBe 1
      }
    }
  }
}


