package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.sql.SQLConversion
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.Unique
import lightdb.{Field, Id, LightDB}
import fabric.rw._

import java.nio.file.Path
import java.sql.ResultSet
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

case class LightDBBench(storeManager: StoreManager) extends Bench { bench =>
  override def name: String = s"LightDB ${storeManager.getClass.getSimpleName.replace("$", "")}"

  override def init(): Unit = DB.init()

  override protected def insertRecords(status: StatusCallback): Int = DB.people.transaction { implicit transaction =>
    (0 until RecordCount)
      .foldLeft(0)((total, index) => {
        val person = Person(
          name = Unique(),
          age = index
        )
        DB.people.set(person)
        status.progress()
        total + 1
      })
  }

  override protected def streamRecords(status: StatusCallback): Int = DB.people.transaction { implicit transaction =>
    (0 until StreamIterations)
      .foldLeft(0)((total, iteration) => {
        var count = 0
        DB.people.iterator.foreach { _ =>
          count += 1
        }
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress()
        total + count
      })
  }

  override protected def searchEachRecord(status: StatusCallback): Int = DB.people.transaction { implicit transaction =>
    var counter = 0
    (0 until StreamIterations)
      .foreach { iteration =>
        (0 until RecordCount)
          .par
          .foreach { index =>
            val list = DB.people.query.filter(_.age === index).search.docs.iterator.toList
            val person = list.head
            if (person.age != index) {
              scribe.warn(s"${person.age} was not $index")
            }
            if (list.size > 1) {
              scribe.warn(s"More than one result for $index")
            }
            counter += 1
            status.progress()
          }
      }
    counter
  }

  override protected def searchAllRecords(status: StatusCallback): Int = DB.people.transaction { implicit transaction =>
    var counter = 0
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        var count = 0
        DB.people.query.search.docs.iterator.foreach { person =>
          count += 1
          counter += 1
        }
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress()
      }
    counter
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