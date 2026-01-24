package spec

import fabric.rw.*
import lightdb.{KeyValue, LightDB}
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.time.Timestamp
import lightdb.transaction.PrefixScanningTransaction
import lightdb.traversal.store.{TraversalKeys, TraversalManager, TraversalPersistedIndex, TraversalStore}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalOrderedPostingsIntersectionSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Keep the environment explicit for this spec.
    System.setProperty("lightdb.traversal.persistedIndex", "true")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.persistedIndex")
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

  case class Person(age: Int,
                    rank: Long,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val age: I[Int] = field.index(_.age)
    val rank: I[Long] = field.index(_.rank)
  }

  specName should {
    "intersect ordered postings streams (eqo prefixes) in docId order" in {
      val docs = List(
        Person(age = 1, rank = 5L, _id = Id("a")),
        Person(age = 1, rank = 6L, _id = Id("b")),
        Person(age = 2, rank = 5L, _id = Id("c")),
        Person(age = 1, rank = 5L, _id = Id("d"))
      )

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[TraversalStore[_, _]]
          .buildPersistedIndex()
        ids <- DB.people.transaction { ttx =>
          val storeName = ttx.store.name

          ttx.store.effectiveIndexBacking match {
            case None =>
              Task { throw new RuntimeException("index backing was not created") }
            case Some(idxStore) =>
              idxStore.transaction { kv0 =>
                val kv = kv0.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
                val p1 = TraversalKeys.eqoPrefix(storeName, Person.age.name, "1")
                val p2 = TraversalKeys.eqoPrefix(storeName, Person.rank.name, "5")
                TraversalPersistedIndex.intersectOrderedPostingsTake(List(p1, p2), kv, 1000)
              }
          }
        }
      yield {
        ids shouldBe List("a", "d")
      }
    }
  }
}



