package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.progress.ProgressManager
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * Field-scoped re-index: rebuild ONE index without rewriting whole documents.
 *
 * Adding an indexed field leaves it empty on existing rows, and the only cure was a full re-index,
 * whose cost is proportional to the document rather than to the change: every column rewritten and
 * every index maintained to populate one. Backfilling a single derived column across 1.4M rows that
 * way was measured at 3% after five hours.
 *
 * The behaviour that matters is not just "the index gets populated" — it is that NOTHING ELSE MOVES.
 * A partial write that quietly reverted a concurrently-changed column would be far worse than a slow
 * backfill, so that is what these assert.
 */
@EmbeddedTest
class SQLiteFieldReIndexSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  case class Person(name: String, age: Int, city: String, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: I[String] = field.index("name", (d: Person) => d.name)
    val age: I[Int] = field.index("age", (d: Person) => d.age)
    val city: I[String] = field.index("city", (d: Person) => d.city)
    /** The stand-in for a newly added derived index: computed from the document, not stored on it. */
    val nameLength: I[Int] = field.index("nameLength", (d: Person) => d.name.length)
  }

  private val specName = getClass.getSimpleName
  private val dbPath: Path = Path.of(s"db/$specName")
  if Files.exists(dbPath) then {
    Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = SQLiteStore
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(dbPath)
    val people: Collection[Person, Person.type] = store(Person)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "field-scoped reIndex" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert the documents" in {
      DB.people.transaction { tx =>
        tx.insert(List(
          Person("Alice", 30, "Denver", Person.id("a")),
          Person("Bartholomew", 41, "Austin", Person.id("b")),
          Person("Cy", 22, "Reno", Person.id("c")),
        )).map(_ => succeed)
      }
    }
    // Per STORE, not per database: an Indexed is typed to its Doc, so it only means anything against
    // the store that declares it. DB.reIndex stays whole-store.
    "rebuild only the named index" in {
      DB.people.reIndex(List(Person.nameLength), ProgressManager.none, None).map(_ should be(true))
    }
    "compute the rebuilt index correctly for every row" in {
      DB.people.transaction { tx =>
        tx.query.filter(_.nameLength === 11).toList.map(_.map(_.name) should be(List("Bartholomew")))
      }
    }
    "find every row by the rebuilt index" in {
      DB.people.transaction { tx =>
        for {
          short <- tx.query.filter(_.nameLength === 2).toList
          five <- tx.query.filter(_.nameLength === 5).toList
        } yield (short.map(_.name), five.map(_.name)) should be((List("Cy"), List("Alice")))
      }
    }
    "leave every OTHER field untouched" in {
      DB.people.transaction { tx =>
        tx.query.toList.map { people =>
          people.sortBy(_.name).map(p => (p.name, p.age, p.city)) should be(List(
            ("Alice", 30, "Denver"),
            ("Bartholomew", 41, "Austin"),
            ("Cy", 22, "Reno"),
          ))
        }
      }
    }
    "write ONLY the named column, leaving a concurrently-changed one alone" in {
      DB.people.transaction { tx =>
        for {
          // Change `city` in the database, then ask for a nameLength-only write with a STALE document
          // that still carries the old city. A whole-document upsert would revert it; a field-scoped
          // write must not.
          before <- tx.get(Person.id("c"))
          _ <- tx.upsert(before.get.copy(city = "Tahoe"))
          _ <- tx.updateFields(before.toList, List(Person.nameLength))
          after <- tx.get(Person.id("c"))
        } yield after.map(_.city) should be(Some("Tahoe"))
      }
    }
    "report how many rows it changed" in {
      DB.people.transaction { tx =>
        for {
          all <- tx.query.toList
          n <- tx.updateFields(all, List(Person.nameLength))
        } yield n should be(3)
      }
    }
    "do nothing, harmlessly, when asked for no indexes" in {
      DB.people.reIndex(Nil, ProgressManager.none, None).map(_ => succeed)
    }
  }
}
