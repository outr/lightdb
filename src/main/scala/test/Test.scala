package test

import cats.effect._
import lightdb.data.JsonDataManager
import lightdb.{Id, LightDB, index}
import lightdb.data.stored._
import lightdb.index.{Indexable, LuceneIndex}
import lightdb.store.HaloStore
import org.apache.lucene.document.{Document, StringField, Field => LuceneField}

object Test extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
//    stored()
//    json()
    indexing()
  }

  private def indexing(): IO[ExitCode] = {
    object ndx extends LuceneIndex[Person]() {
      val name = field[String]("name", _.name)
    }

    ndx.index(Id("people", "indexing-example1"), Person("Test Indexing", 123))
    ndx.flush()



    implicit object StringIndexable extends Indexable[String] {
      override def index[T](doc: Document, name: String, value: String): Unit = doc.add(new StringField(name, value, LuceneField.Store.YES))
    }
  }

  private def json(): IO[ExitCode] = {
    val dataManager = new JsonDataManager[Person]
    val db = new LightDB(new HaloStore)
    val collection = db.collection[Person](dataManager)
    val io: IO[Option[Person]] = collection.modify(Id[Person]("person", "test2")) {
      case Some(p) => {
        scribe.info(s"Name: ${p.name}, Age: ${p.age}")
        Some(p.copy(age = p.age + 1))
      }
      case None => {
        scribe.info(s"No record found!")
        Some(Person(name = "Matt Hicks", age = 41))
      }
    }
    for {
      result <- io
      _ <- db.dispose()
    } yield {
      scribe.info(s"Result: $result")
      ExitCode.Success
    }
  }

  private def stored(): IO[ExitCode] = {
    val t = StoredType(Vector(
      ValueTypeEntry("name", StringType),
      ValueTypeEntry("age", IntType)
    ))
    val dataManager = new StoredDataManager(t)

    val db = new LightDB(new HaloStore)
    val collection = db.collection[Stored](dataManager)
    val io: IO[Option[Stored]] = collection.modify(Id[Stored]("person", "test1")) {
      case Some(s) => {
        scribe.info(s"Name: ${s("name")}, Age: ${s("age")}")
        Some(t.create("name" -> "Matt Hicks", "age" -> (s[Int]("age") + 1)))
      }
      case None => {
        scribe.info("No record found!")
        Some(t.create("name" -> "Matt Hicks", "age" -> 41))
      }
    }
    for {
      result <- io
      _ <- db.dispose()
    } yield {
      scribe.info(s"Result: $result")
      ExitCode.Success
    }
  }
}

case class Person(name: String, age: Int)

object Person {
  implicit val rw: ReadWriter[Person] = macroRW
}

trait PersonFields extends Fields[Person] {
  val name: Field[String, Person] = field[String]("name")
  val age: Field[Int, Person] = field[Int]("age")
}

trait Fields[P] {
  private var _fields = Map.empty[String, Field[_, P]]

  def fields: Map[String, Field[_, P]] = _fields

  def field[F](name: String): Field[F, P] = {
    val f = Field[F, P](name)
    synchronized {
      _fields += name -> f
    }
    f
  }
}

case class Field[F, P](name: String)