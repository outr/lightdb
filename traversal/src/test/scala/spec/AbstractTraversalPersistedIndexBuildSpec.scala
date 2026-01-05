package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.traversal.store.{TraversalManager, TraversalTransaction}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}

/**
 * Abstract spec validating persisted index build + postings exposure for TraversalStore.
 *
 * Concrete store modules should extend this and provide `traversalStoreManager`.
 */
abstract class AbstractTraversalPersistedIndexBuildSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
{
  def traversalStoreManager: TraversalManager

  private lazy val specName: String = getClass.getSimpleName

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager
    override lazy val directory: Option[Path] = Some(Files.createTempDirectory("lightdb-traversal-persisted-index-build-"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val entries: S[Entry, Entry.type] = store(Entry, name = Some("entries"))
  }

  specName should {
    "initialize" in DB.init.succeed

    "build persisted index and expose ready + postings (eq + ngram)" in {
      DB.entries.transaction { tx =>
        val store = tx.store
        for {
          _ <- tx.insert(List(
            Entry(name = "Alice", age = 10, _id = Id("a")),
            Entry(name = "Bob", age = 30, _id = Id("b")),
            Entry(name = "Alice", age = 20, _id = Id("c"))
          ))

          readyBefore <- store.persistedIndexReady()
          _ <- store.buildPersistedIndex()
          readyAfter <- store.persistedIndexReady()

          eq <- store.persistedEqPostings(fieldName = "name", value = "Alice")
          ng <- store.persistedNgPostings(fieldName = "name", query = "lic")
          sw <- store.persistedSwPostings(fieldName = "name", query = "Al")
          ew <- store.persistedEwPostings(fieldName = "name", query = "ice")
          rl <- store.persistedRangeLongPostings(fieldName = "age", from = Some(5L), to = Some(15L))
          rl2 <- store.persistedRangeLongPostings(fieldName = "age", from = Some(15L), to = Some(25L))
          rlFromOnly <- store.persistedRangeLongPostings(fieldName = "age", from = Some(15L), to = None)
        } yield {
          // New DBs start with an empty store, so "ready" is set immediately, and write-through indexing keeps it ready.
          readyBefore shouldBe true
          readyAfter shouldBe true
          eq shouldBe Set("a", "c")
          ng shouldBe Set("a", "c")
          sw shouldBe Set("a", "c")
          ew shouldBe Set("a", "c")
          rl shouldBe Set("a")
          rl2 shouldBe Set("c")
          // One-sided range seeding is disabled by default to avoid "seed nearly everything" at scale.
          rlFromOnly shouldBe Set.empty
        }
      }
    }
  }
}

case class Entry(name: String, age: Int, _id: Id[Entry] = Id()) extends Document[Entry]

object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
  override implicit val rw: RW[Entry] = RW.gen
  val name: I[String] = field.index("name", _.name)
  val age: I[Int] = field.index("age", _.age)
}


