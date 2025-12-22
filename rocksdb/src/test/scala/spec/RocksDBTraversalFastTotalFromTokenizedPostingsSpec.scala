package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalFastTotalFromTokenizedPostingsSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Force seed materialization to be skipped so totals must use postings-count fast path.
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
                    bio: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: F[String] = field("name", _.name)
    val bio = field.tokenized("bio", _.bio)
  }

  specName should {
    "compute total from token postings count for single-token tokenized Equals when seed materialization is disabled" in {
      val docs = List(
        Person("a", "quick fox", _id = Id("a")),
        Person("b", "quick brown", _id = Id("b")),
        Person("c", "something else", _id = Id("c"))
      )
      for {
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .countTotal(true)
            .limit(1)
            .filter(_.bio === "quick")
            .search
        }
      } yield {
        results.total shouldBe Some(2)
      }
    }

    "compute total from token postings intersection for multi-token tokenized Equals when seed materialization is disabled" in {
      val docs = List(
        Person("a", "quick fox", _id = Id("a")),
        Person("b", "quick brown fox", _id = Id("b")),
        Person("c", "quick brown", _id = Id("c")),
        Person("d", "something else", _id = Id("d"))
      )
      for {
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .countTotal(true)
            .limit(1)
            .filter(_.bio === "quick fox")
            .search
        }
      } yield {
        results.total shouldBe Some(2)
      }
    }
  }
}


