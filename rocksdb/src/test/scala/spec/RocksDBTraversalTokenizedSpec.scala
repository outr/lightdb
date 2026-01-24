package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec
import lightdb.filter.FilterExtras

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalTokenizedSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Force candidate materialization bounds low so we exercise tok-based seeding on small sets.
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
                    rank: Long = 0L,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: F[String] = field("name", _.name)
    val bio = field.tokenized("bio", _.bio)
    val rank: I[Long] = field.index(_.rank)
  }

  specName should {
    "support tokenized Equals and NotEquals semantics (all tokens must match)" in {
      val docs = List(
        Person("a", "The quick brown fox", _id = Id("a")),
        Person("b", "Quick FOX jumps", _id = Id("b")),
        Person("c", "just quick", _id = Id("c")),
        Person("d", "something else", _id = Id("d"))
      )

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        eqNames <- DB.people.transaction { tx =>
          tx.query.filter(_.bio === "quick fox").toList.map(_.map(_.name).sorted)
        }
        neNames <- DB.people.transaction { tx =>
          tx.query.filter(_.bio !== "quick fox").toList.map(_.map(_.name).sorted)
        }
      yield {
        eqNames shouldBe List("a", "b")
        neNames.toSet shouldBe Set("c", "d")
      }
    }

    "support tokenized Equals with page-only IndexOrder streaming seed" in {
      val docs = List(
        Person("a", "The quick brown fox", _id = Id("a")),
        Person("b", "Quick FOX jumps", _id = Id("b")),
        Person("c", "just quick", _id = Id("c")),
        Person("d", "something else", _id = Id("d"))
      )

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        page1 <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(lightdb.Sort.IndexOrder)
            .limit(1)
            .offset(0)
            .filter(_.bio === "quick fox")
            .toList
            .map(_.map(_.name))
        }
        page2 <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(lightdb.Sort.IndexOrder)
            .limit(1)
            .offset(1)
            .filter(_.bio === "quick fox")
            .toList
            .map(_.map(_.name))
        }
      yield {
        page1 shouldBe List("a")
        page2 shouldBe List("b")
      }
    }

    "support tokenized Equals page-only when first token is very common (should still page correctly)" in {
      val common = (1 to 500).toList.map(i => Person(s"c$i", bio = "common", _id = Id(s"c$i")))
      val hits = List(
        Person("a", bio = "common rare", _id = Id("a")),
        Person("b", bio = "common rare", _id = Id("b"))
      )
      val docs = common ++ hits

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        page1 <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(lightdb.Sort.IndexOrder)
            .limit(1)
            .offset(0)
            // Intentionally place the common token first.
            .filter(_.bio === "common rare")
            .toList
            .map(_.map(_.name))
        }
        page2 <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(lightdb.Sort.IndexOrder)
            .limit(1)
            .offset(1)
            .filter(_.bio === "common rare")
            .toList
            .map(_.map(_.name))
        }
      yield {
        page1 shouldBe List("a")
        page2 shouldBe List("b")
      }
    }

    "support tokenized Equals inside Multi streaming seed (driver selection + IndexOrder)" in {
      val docs = List(
        Person("a", "The quick brown fox", rank = 1L, _id = Id("a")),
        Person("b", "Quick FOX jumps", rank = 1L, _id = Id("b")),
        Person("c", "just quick", rank = 1L, _id = Id("c")),
        Person("d", "something else", rank = 1L, _id = Id("d"))
      )

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        page <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(lightdb.Sort.IndexOrder)
            .limit(2)
            .filter(p => (p.rank === 1L) && (p.bio === "quick fox"))
            .toList
            .map(_.map(_.name))
        }
      yield {
        page shouldBe List("a", "b")
      }
    }
  }
}


