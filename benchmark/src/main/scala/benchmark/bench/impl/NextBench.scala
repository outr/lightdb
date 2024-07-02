package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.sql.SQLiteStore
import lightdb.util.Unique
import lightdb.{Converter, Field, collection}

import java.nio.file.Path
import java.sql.ResultSet
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

object NextBench extends Bench {
  override def name: String = "NextBench-Converter"

  override def init(): Unit = people.init()

  override protected def insertRecords(status: StatusCallback): Int = people.transaction { implicit transaction =>
    (0 until RecordCount)
      .foldLeft(0)((total, index) => {
        val person = Person(
          name = Unique(),
          age = index,
          id = Unique()
        )
        people.set(person)
        status.progress.set(index + 1)
        total + 1
      })
  }

  override protected def streamRecords(status: StatusCallback): Int = people.transaction { implicit transaction =>
    (0 until StreamIterations)
      .foldLeft(0)((total, iteration) => {
        var count = 0
        people.iterator.foreach { person =>
          count += 1
        }
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress.set(iteration + 1)
        total + count
      })
  }

  override protected def searchEachRecord(status: StatusCallback): Int = people.transaction { implicit transaction =>
    var counter = 0
    (0 until StreamIterations)
      .foreach { iteration =>
        (0 until RecordCount)
          .foreach { index =>
            val list = people.query.filter(_.age === index).search.docs.iterator.toList
            val person = list.head
            if (person.age != index) {
              scribe.warn(s"${person.age} was not $index")
            }
            if (list.size > 1) {
              scribe.warn(s"More than one result for $index")
            }
            counter += 1
            status.progress.set((iteration + 1) * (index + 1))
          }
      }
    counter
  }

  override protected def searchAllRecords(status: StatusCallback): Int = people.transaction { implicit transaction =>
    var counter = 0
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        var count = 0
        people.query.search.docs.iterator.foreach { person =>
          count += 1
          counter += 1
        }
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress.set(iteration + 1)
      }
    counter
  }

  override def size(): Long = -1L

  override def dispose(): Unit = people.dispose()

  val people: Collection[Person, Person.type] = collection.Collection("people", Person, SQLiteStore(Path.of("db/people.db"), PersonConverter), cacheQueries = true)

  case class Person(name: String, age: Int, id: String)

  object Person extends DocModel[Person] {
    val name: Field.Basic[Person, String] = field("name", _.name)
    val age: Field.Index[Person, Int] = field.index("age", _.age)
    val id: Field.Unique[Person, String] = field.unique("id", _.id)
  }

  object PersonConverter extends Converter[ResultSet, Person] {
    override def convert(rs: ResultSet): Person = Person(
      name = rs.getString("name"),
      age = rs.getInt("age"),
      id = rs.getString("id")
    )
  }
}
