package test

import cats.effect._
import lightdb.data.JsonDataManager
import lightdb.{Id, LightDB}
import lightdb.data.stored._
import lightdb.store.HaloStore

import profig._

object Test extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
//    stored()
    json()
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