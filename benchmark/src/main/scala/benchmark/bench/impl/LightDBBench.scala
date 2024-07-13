package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.sql.SQLConversion
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Field, Id, LightDB}
import fabric.rw._

import java.nio.file.Path
import java.sql.ResultSet
import scala.language.implicitConversions

case class LightDBBench(storeManager: StoreManager) extends Bench { bench =>
  override def name: String = s"LightDB ${storeManager.getClass.getSimpleName.replace("$", "")}"

  override def init(): Unit = DB.init()

  implicit def p2Person(p: P): Person = Person(p.name, p.age, Id(p.id))

  def toP(person: Person): P = P(person.name, person.age, person._id.value)

  override protected def insertRecords(iterator: Iterator[P]): Unit = DB.people.transaction { implicit transaction =>
    iterator.foreach { p =>
      val person: Person = p
      DB.people.insert(person)
    }
  }

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { implicit transaction =>
    f(DB.people.iterator.map(toP))
  }

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = DB.people.transaction { implicit transaction =>
    ageIterator.foreach { age =>
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
    }
  }

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { implicit transaction =>
    val iterator = DB.people.query.search.docs.iterator.map(toP)
    f(iterator)
  }

  override def size(): Long = DB.people.store.size

  override def dispose(): Unit = DB.people.dispose()

  object DB extends LightDB {
    override lazy val directory: Option[Path] = Some(Path.of(s"db/${storeManager.getClass.getSimpleName.replace("$", "")}"))

    val people: Collection[Person, Person.type] = collection(Person, cacheQueries = true)

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

    val name: Field[Person, String] = field("name", _.name)
    val age: Field.Index[Person, Int] = field.index("age", _.age)
  }
}