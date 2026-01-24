package spec

import fabric.rw.*
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
class RocksDBTraversalFastTotalFromPrefixPostingsSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Force seed materialization to be skipped so totals must use postings-count fast path.
    System.setProperty("lightdb.traversal.persistedIndex.maxSeedSize", "1")
    // Opt-in: prefix postings totals are only exact if string values are consistently normalized.
    System.setProperty("lightdb.traversal.persistedIndex.fastTotal.prefix.enabled", "true")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.persistedIndex.maxSeedSize")
      System.clearProperty("lightdb.traversal.persistedIndex.fastTotal.prefix.enabled")
    }
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
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  specName should {
    "compute total from prefix postings count for StartsWith when seed materialization is disabled" in {
      val docs =
        (1 to 50).toList.map(i => Person(name = if i <= 40 then s"abc$i" else s"zzz$i", _id = Id(s"p$i")))
      for
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
            .filter(_.name.startsWith("abc"))
            .search
        }
      yield {
        results.total shouldBe Some(40)
      }
    }

    "compute total from prefix postings count for EndsWith when seed materialization is disabled" in {
      val docs =
        (1 to 50).toList.map(i => Person(name = if i <= 12 then s"p${i}_xyz" else s"p${i}_zzz", _id = Id(s"e$i")))
      for
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
            .filter(_.name.endsWith("xyz"))
            .search
        }
      yield {
        results.total shouldBe Some(12)
      }
    }
  }
}


