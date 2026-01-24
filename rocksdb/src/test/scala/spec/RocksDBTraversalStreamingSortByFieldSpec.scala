package spec

import fabric.rw.*
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
class RocksDBTraversalStreamingSortByFieldSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    System.setProperty("lightdb.traversal.streamingSortByField.enabled", "true")
    System.setProperty("lightdb.traversal.orderByFieldPostings.enabled", "true")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.streamingSortByField.enabled")
      System.clearProperty("lightdb.traversal.orderByFieldPostings.enabled")
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
    "stream a page using persisted numeric order for Sort.ByField (desc)" in {
      val docs = List(
        Person("a", 10, _id = Id("a")),
        Person("b", 30, _id = Id("b")),
        Person("c", 20, _id = Id("c")),
        Person("d", 40, _id = Id("d"))
      )

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        names <- DB.people.transaction { tx =>
          tx.query.sort(Sort.ByField(Person.age).desc).limit(3).toList.map(_.map(_.name))
        }
      yield {
        names shouldBe List("d", "b", "c")
      }
    }
  }
}


