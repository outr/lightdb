package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/** Regression test for the polytype-document-root round-trip bug.
  *
  * When a Document model's RW is a [[fabric.rw.PolyType]] (open-hierarchy
  * sealed trait + dynamic subtype registration — the shape used by Sigil's
  * `Event`, `Tool`, `SpaceId`, etc.), the SQL store must persist fabric's
  * `type` discriminator alongside the projected subtype fields and replay
  * it back on read. Without it, `getDoc`'s reconstructed JSON has every
  * subtype field but no `type`, and fabric's polymorphic dispatch throws
  * `RuntimeException: Lookup not found: type` on the next read.
  *
  * Insert one of each subtype, list them back, assert both round-trip.
  * Pre-fix the `toList` step throws. Post-fix both subtypes survive. */
@EmbeddedTest
class PostgreSQLPolyRootSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = "PostgreSQLPolyRootSpec"

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert a Dog and a Cat" in {
      DB.animals.transaction(_.insert(List(
        Dog(name = "Rex", barkLoudness = 7, _id = Id("rex")),
        Cat(name = "Whiskers", livesRemaining = 9, _id = Id("whiskers"))
      ))).succeed
    }
    "list animals back, preserving subtype identity" in {
      DB.animals.transaction { transaction =>
        transaction.stream.toList.map { list =>
          list.collect { case d: Dog => d }.map(_.name).toSet should be(Set("Rex"))
          list.collect { case c: Cat => c }.map(_.name).toSet should be(Set("Whiskers"))
          list.collect { case d: Dog => d.barkLoudness }.toSet should be(Set(7))
          list.collect { case c: Cat => c.livesRemaining }.toSet should be(Set(9))
        }
      }
    }
    "get a Dog by id and preserve subtype" in {
      DB.animals.transaction { transaction =>
        transaction.get(Id[Animal]("rex")).map { animal =>
          animal.exists(_.isInstanceOf[Dog]) should be(true)
          animal.collect { case d: Dog => d.barkLoudness } should be(Some(7))
        }
      }
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  lazy val storeManager: CollectionManager = PostgreSQLStoreManager(HikariConnectionManager(SQLConfig(
    jdbcUrl = "jdbc:postgresql://localhost:5432/basic",
    username = Some("postgres"),
    password = Some("password")
  )))

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val animals: Collection[Animal, Animal.type] = store(Animal)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  /** Open polymorphic hierarchy — mirrors how Sigil declares `Event` /
    * `Tool` / `SpaceId`: a trait with a `PolyType` companion, concrete
    * subtypes registered at startup. */
  trait Animal extends RecordDocument[Animal] {
    def name: String
  }

  case class Dog(name: String,
                 barkLoudness: Int,
                 _id: Id[Animal] = Animal.id(),
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp()) extends Animal derives RW

  case class Cat(name: String,
                 livesRemaining: Int,
                 _id: Id[Animal] = Animal.id(),
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp()) extends Animal derives RW

  object Animal extends PolyType[Animal]()(using scala.reflect.ClassTag(classOf[Animal]))
    with RecordDocumentModel[Animal]
    with JsonConversion[Animal] {
    register(summon[RW[Dog]], summon[RW[Cat]])

    // Expose PolyType's underlying RW as the model's `rw`
    implicit override val rw: RW[Animal] = polyRW

    // Local val renamed to avoid clash with `PolyType.name` (the typed-name
    // namespace on PolyType). Column on the wire is still "name".
    val nameField: F[String] = field("name", (a: Animal) => a.name)
  }
}
