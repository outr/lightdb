package benchmark.bench.impl

import benchmark.bench.Bench
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.sql.SQLConversion
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.id.Id
import rapid.Task

import java.nio.file.Path
import java.sql.ResultSet
import scala.language.implicitConversions

case class LightDBBench(storeManager: StoreManager) extends Bench { bench =>
  override def name: String = s"LightDB ${storeManager.name}"

  override def init(): Unit = DB.init.sync()

  implicit def p2Person(p: P): Person = Person(p.name, p.age, Id(p.id))

  def toP(person: Person): P = P(person.name, person.age, person._id.value)

  override protected def insertRecords(iterator: Iterator[P]): Unit = DB.people.transaction { transaction =>
    rapid.Stream.fromIterator(Task(iterator))
      .evalMap(p => DB.people.insert(p))
      .drain
  }.sync()

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { transaction =>
//    f(DB.people.iterator.map(toP))
    ???
  }

  override protected def getEachRecord(idIterator: Iterator[String]): Unit = DB.people.transaction { transaction =>
    /*idIterator.foreach { idString =>
      val id = Person.id(idString)
      DB.people.get(id) match {
        case Some(person) =>
          if (person._id.value != idString) {
            scribe.warn(s"${person._id.value} was not $id")
          }
        case None => scribe.warn(s"$id was not found!")
      }
    }*/
    ???
  }

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = DB.people.transaction { transaction =>
    /*ageIterator.foreach { age =>
      try {
        val list = DB.people.query.filter(_.age === age).search.docs.list
        val person = list.head
        if (person.age != age) {
          scribe.warn(s"${person.age} was not $age")
        }
        if (list.size > 1) {
          scribe.warn(s"More than one result for $age")
        }
      } catch {
        case t: Throwable => throw new RuntimeException(s"Error with $age", t)
      }
    }*/
    ???
  }

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { transaction =>
//    val iterator = DB.people.query.search.docs.iterator.map(toP)
//    f(iterator)
    ???
  }

  override def size(): Long = -1L

  override def dispose(): Unit = DB.people.dispose()

  object DB extends LightDB {
    Store.CacheQueries = true

    override lazy val directory: Option[Path] = Some(Path.of(s"db/${storeManager.getClass.getSimpleName.replace("$", "")}"))

    val people: Store[Person, Person.type] = store(Person)

    override def storeManager: StoreManager = bench.storeManager
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