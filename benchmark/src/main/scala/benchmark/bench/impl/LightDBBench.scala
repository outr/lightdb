package benchmark.bench.impl

import benchmark.bench.Bench
import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter._
import lightdb.id.Id
import lightdb.sql.SQLConversion
import lightdb.store.{CollectionManager, Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import rapid.Task

import java.nio.file.Path
import java.sql.ResultSet
import scala.language.implicitConversions

case class LightDBBench(storeManager: CollectionManager) extends Bench { bench =>
  override def name: String = s"LightDB ${storeManager.name}"

  override def init(): Unit = DB.init.sync()

  implicit def p2Person(p: P): Person = Person(p.name, p.age, Id(p.id))

  def toP(person: Person): P = P(person.name, person.age, person._id.value)

  override protected def insertRecords(iterator: Iterator[P]): Unit = DB.people.transaction { transaction =>
    rapid.Stream.fromIterator(Task(iterator))
      .evalMap(p => transaction.insert(p))
      .drain
  }.sync()

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { transaction =>
    transaction.list.map { list =>
      f(list.map(toP).iterator)
      list
    }
  }.sync()

  override protected def getEachRecord(idIterator: Iterator[String]): Unit = DB.people.transaction { transaction =>
    Task.defer {
      idIterator.foreach { idString =>
        val id = Person.id(idString)
        transaction.get(id).sync() match {
          case Some(person) =>
            if (person._id.value != idString) {
              scribe.warn(s"${person._id.value} was not $id")
            }
          case None => scribe.warn(s"$id was not found!")
        }
      }
      Task.unit
    }
  }.sync()

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = DB.people.transaction { transaction =>
    Task.defer {
      ageIterator.foreach { age =>
        try {
          val list = transaction.query.filter(_.age.is(age)).docs.toList.sync()
          if (list.nonEmpty) {
            val person = list.head
            if (person.age != age) {
              scribe.warn(s"${person.age} was not $age")
            }
            if (list.size > 1) {
              scribe.warn(s"More than one result for $age")
            }
          } else {
            scribe.warn(s"No results for age $age")
          }
        } catch {
          case t: Throwable => throw new RuntimeException(s"Error with $age", t)
        }
      }
      Task.unit
    }
  }.sync()

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { transaction =>
    transaction.list.map { list =>
      f(list.map(toP).iterator)
      list
    }
  }.sync()

  override def size(): Long = -1L

  override def dispose(): Unit = DB.people.dispose()

  object DB extends LightDB {
    type SM = CollectionManager

    Store.CacheQueries = true

    override lazy val directory: Option[Path] = Some(Path.of(s"db/${storeManager.getClass.getSimpleName.replace("$", "")}"))

    val people: S[Person, Person.type] = store[Person, Person.type](Person)

    override val storeManager: SM = bench.storeManager
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with SQLConversion[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    override def convertFromSQL(rs: ResultSet): Person = Person(
      name = rs.getString("name"),
      age = rs.getInt("age"),
      _id = id(rs.getString("_id"))
    )

    val name: F[String] = field("name", _.name)
    val age: I[Int] = field.index("age", _.age)
  }
}
