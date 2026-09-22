package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.indexeddb.IndexedDBStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.Path
import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

/** Installs an in-memory IndexedDB implementation as `globalThis.indexedDB` when the tests run in Node. */
@js.native
@JSImport("fake-indexeddb/auto", JSImport.Namespace)
object FakeIndexedDB extends js.Object

case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

object Person extends DocumentModel[Person] with JsonConversion[Person] {
  override implicit val rw: RW[Person] = RW.gen

  val name: I[String] = field.index(_.name)
  val age: I[Int] = field.index(_.age)
}

class BrowserDB extends LightDB {
  override type SM = IndexedDBStore.type
  override val storeManager: IndexedDBStore.type = IndexedDBStore

  override def name: String = "IndexedDBStoreSpec"
  override def directory: Option[Path] = None
  override def upgrades: List[DatabaseUpgrade] = Nil

  val people: IndexedDBStore[Person, Person.type] = store(Person)()
  lazy val counter = stored[Int]("counter", 0)
}

class IndexedDBStoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  require(!js.isUndefined(FakeIndexedDB), "fake-indexeddb is required: run `npm install` in the indexeddb directory")

  private var db = new BrowserDB

  // rapid's Stream.toList / count / drain evaluate pulls synchronously (Task.quickEval), which cannot wait for
  // IndexedDB on Scala.js. `fold` is fully task-based, so the spec collects streams with it. Likewise the
  // `insert(Seq)` overload folds through `Stream.count` internally, so records are inserted one at a time.
  private def listOf[A](stream: rapid.Stream[A]): Task[List[A]] =
    stream.fold(List.empty[A])((acc, a) => Task.pure(a :: acc)).map(_.reverse)

  private def insertAll(tx: lightdb.transaction.Transaction[Person, Person.type], people: List[Person]): Task[Unit] =
    people.foldLeft(Task.unit)((work, p) => work.next(tx.insert(p)).unit)

  private val adam = Person("Adam", 21, Id("adam"))
  private val brenda = Person("Brenda", 11, Id("brenda"))
  private val charlie = Person("Charlie", 35, Id("charlie"))

  "IndexedDBStore" should {
    "initialize the database" in {
      db.init.map(_ => succeed)
    }
    "insert records and read them back by id" in {
      db.people.transaction(tx => insertAll(tx, List(adam, brenda, charlie))).flatMap { _ =>
        db.people.transaction(_.get(Id[Person]("brenda")))
      }.map(_ should be(Some(brenda)))
    }
    "count and stream every record" in {
      db.people.transaction { tx =>
        tx.count.flatMap(count => listOf(tx.stream).map(count -> _))
      }.map { (count, list) =>
        count should be(3)
        list.map(_.name).toSet should be(Set("Adam", "Brenda", "Charlie"))
      }
    }
    "read its own writes and hide them from other transactions until commit" in {
      val dave = Person("Dave", 40, Id("dave"))
      db.people.transaction { tx =>
        for
          _ <- tx.insert(dave)
          own <- tx.get(dave._id)
          other <- db.people.transaction(_.get(dave._id))
        yield (own, other)
      }.flatMap { (own, other) =>
        db.people.transaction(_.get(dave._id)).map(committed => (own, other, committed))
      }.map { (own, other, committed) =>
        own should be(Some(dave))
        other should be(None)
        committed should be(Some(dave))
      }
    }
    "discard every write of a transaction whose body fails" in {
      val erin = Person("Erin", 29, Id("erin"))
      db.people.transaction { tx =>
        tx.insert(erin).next(tx.delete(adam._id)).next(Task.error(new RuntimeException("body failed")))
      }.attempt.flatMap { result =>
        result.isFailure should be(true)
        db.people.transaction(tx => tx.get(erin._id).flatMap(e => tx.get(adam._id).map(e -> _)))
      }.map { (erinAfter, adamAfter) =>
        erinAfter should be(None)
        adamAfter should be(Some(adam))
      }
    }
    "delete a record" in {
      db.people.transaction(_.delete(Id[Person]("dave"))).flatMap { deleted =>
        db.people.transaction(_.count).map(deleted -> _)
      }.map { (deleted, count) =>
        deleted should be(true)
        count should be(3)
      }
    }
    "serialize concurrent StoredValue.modify calls across asynchronous boundaries" in {
      (1 to 10).toList.map(_ => db.counter.modify(_ + 1)).tasksPar.flatMap(_ => db.counter.get()).map(_ should be(10))
    }
    "persist records across a dispose and reopen" in {
      db.dispose.flatMap { _ =>
        db = new BrowserDB
        db.init
      }.flatMap { _ =>
        db.people.transaction(tx => listOf(tx.stream)).flatMap(people => db.counter.get().map(people -> _))
      }.map { (people, counter) =>
        people.map(_.name).toSet should be(Set("Adam", "Brenda", "Charlie"))
        counter should be(10)
      }
    }
    "truncate the store" in {
      db.people.transaction(_.truncate).flatMap { removed =>
        db.people.transaction(_.count).map(removed -> _)
      }.map { (removed, count) =>
        removed should be(3)
        count should be(0)
      }
    }
    "dispose the database" in {
      db.dispose.map(_ => succeed)
    }
  }
}
