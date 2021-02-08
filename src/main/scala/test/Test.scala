package test

import cats.effect._
import lightdb.{Id, LightDB}
import lightdb.data.stored._
import lightdb.store.HaloStore

object Test extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val t = StoredType(Vector(
      ValueTypeEntry("name", StringType),
      ValueTypeEntry("age", IntType)
    ))
    val db = new LightDB(new HaloStore)
    val collection = db.collection[Stored](new StoredDataManager(t))
    val io = collection.modify(Id[Stored]("person", "test1")) {
      case Some(s) => {
        println(s"Name: ${s("name")}, Age: ${s("age")}")
        Some(t.create("name" -> "Matt Hicks", "age" -> (s[Int]("age") + 1)))
      }
      case None => {
        println("No record found!")
        Some(t.create("name" -> "Matt Hicks", "age" -> 41))
      }
    }
    for {
      result <- io
      _ <- db.dispose()
    } yield {
      println(s"Result: $result")
      ExitCode.Success
    }
  }
}