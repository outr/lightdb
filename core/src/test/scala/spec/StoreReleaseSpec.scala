package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.hashmap.HashMapStore
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.lang.ref.WeakReference
import java.nio.file.Path
import scala.concurrent.duration.*

/**
 * A document model outlives the databases that use it, so it must not keep them: once a database is disposed and its
 * owner lets go of it, nothing — not the model its stores were initialized with — keeps it, or the documents in it,
 * reachable. Proved by a weak reference that the collector clears.
 */
@EmbeddedTest
class StoreReleaseSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import StoreReleaseSpec.*

  /** Open a database, write to it, dispose it, and hand back only weak references to it and its store. */
  private def useAndDispose(): Task[(WeakReference[AnyRef], WeakReference[AnyRef])] = {
    val db = new DB
    for {
      _ <- db.init
      _ <- db.notes.transaction(_.insert(Note("kept only while the database lives")))
      _ <- db.dispose
    } yield (new WeakReference[AnyRef](db), new WeakReference[AnyRef](db.notes))
  }

  private def cleared(ref: WeakReference[AnyRef], until: Long): Task[Boolean] =
    if (ref.get() == null) Task.pure(true)
    else if (System.currentTimeMillis() > until) Task.pure(false)
    else Task { System.gc() }.flatMap(_ => Task.sleep(100.millis)).flatMap(_ => cleared(ref, until))

  "A disposed database" should {
    "be released by the model its stores were initialized with" in {
      // Run to completion first, so no step of the task that used the database is still held while the collector runs.
      val (dbRef, storeRef) = useAndDispose().sync()
      val deadline = System.currentTimeMillis() + 10000L
      for {
        dbGone <- cleared(dbRef, deadline)
        storeGone <- cleared(storeRef, deadline)
      } yield {
        storeGone shouldBe true
        dbGone shouldBe true
      }
    }

    "let a second database initialize a store of the same model" in {
      val db = new DB
      for {
        _ <- db.init
        _ <- db.notes.transaction(_.insert(Note("second")))
        count <- db.notes.transaction(_.count)
        _ <- db.dispose
      } yield count shouldBe 1
    }
  }
}

object StoreReleaseSpec {
  case class Note(text: String, _id: Id[Note] = Note.id()) extends Document[Note]

  object Note extends DocumentModel[Note] with JsonConversion[Note] {
    override implicit val rw: RW[Note] = RW.gen
    val text: I[String] = field.index(_.text)
  }

  class DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore

    override def name: String = "StoreReleaseSpec"

    lazy val directory: Option[Path] = None

    // The owner disposes it; no JVM shutdown hook holds it.
    override protected def disposeOnShutdown: Boolean = false

    val notes: Store[Note, Note.type] = store(Note)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
