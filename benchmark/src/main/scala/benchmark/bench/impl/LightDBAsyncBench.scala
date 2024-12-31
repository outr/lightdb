package benchmark.bench.impl

import benchmark.bench.Bench
import fabric.rw.RW
import lightdb.Id
import lightdb.async.{AsyncCollection, AsyncDatabaseUpgrade, AsyncLightDB}
import lightdb.collection.Collection
import lightdb.store.StoreManager
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.sql.SQLConversion
import rapid.Task

import java.nio.file.Path
import java.sql.ResultSet

case class LightDBAsyncBench(storeManager: StoreManager) extends Bench { bench =>
  override def name: String = s"LightDB Async ${storeManager.name}"

  override def init(): Unit = db.init.sync()

  implicit def p2Person(p: P): Person = Person(p.name, p.age, Id(p.id))

  def toP(person: Person): P = P(person.name, person.age, person._id.value)

  override protected def insertRecords(iterator: Iterator[P]): Unit = DB.people.transaction { implicit transaction =>
    rapid.Stream.fromIterator(Task(iterator)).evalMap { p =>
      DB.people.insert(p)
    }.drain
  }.sync()

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { implicit transaction =>
    Task(f(DB.people.underlying.iterator.map(toP)))
  }.sync()

  override protected def getEachRecord(idIterator: Iterator[String]): Unit = DB.people.transaction { implicit transaction =>
    rapid.Stream.fromIterator(Task(idIterator))
      .evalMap { idString =>
        val id = Person.id(idString)
        DB.people.get(id).map {
          case Some(person) =>
            if (person._id.value != idString) {
              scribe.warn(s"${person._id.value} was not $id")
            }
          case None => scribe.warn(s"$id was not found")
        }
      }
      .drain
  }.sync()

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = DB.people.transaction { implicit transaction =>
    rapid.Stream.fromIterator(Task(ageIterator))
      .evalMap { age =>
        DB.people.query.filter(_.age === age).one.map { person =>
          if (person.age != age) {
            scribe.warn(s"${person.age} was not $age")
          }
        }
      }
      .drain
  }.sync()

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = DB.people.transaction { implicit transaction =>
    Task {
      val iterator = DB.people.underlying.query.search.docs.iterator.map(toP)
      f(iterator)
    }
  }.sync()

  override def size(): Long = -1L

  override def dispose(): Unit = DB.people.dispose().sync()

  object DB extends AsyncLightDB {
    Collection.CacheQueries = true

    override def directory: Option[Path] = Some(Path.of(s"db/Async${storeManager.getClass.getSimpleName.replace("$", "")}"))

    val people: AsyncCollection[Person, Person.type] = collection(Person)

    override def storeManager: StoreManager = bench.storeManager
    override def upgrades: List[AsyncDatabaseUpgrade] = Nil
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
