package testdb

import java.io.File
import com.oath.halodb.{HaloDB, HaloDBOptions}
import io.circe.{Json, Printer}
import io.youi.Unique

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

import scribe.Execution.global

object Test {
  def main(args: Array[String]): Unit = {
    val t = StoredType(Vector(
      ValueTypeEntry("name", StringType),
      ValueTypeEntry("age", IntType)
    ))
//    val stored1 = t.create("name" -> "Hello, World", "age" -> 26)
//
//    val stored2 = t(stored1.bb.array())
//    println(s"Name: [${stored2("name")}]")
//    println(s"Age: [${stored2("age")}]")

    val db = new LightDB(new HaloStore)
//    val future = db.store.modify(Id[String]("testing", "hello")) { option =>
//      scribe.info(s"Existing? ${option.map(array => new String(array, "UTF-8"))}")
//      Some("Hello, World".getBytes("UTF-8"))
//    }
    val collection = db.collection[Stored](new StoredDataManager(t))
    val future = collection.modify(Id[Stored]("person", "test1")) { existing =>
      existing match {
        case Some(s) => {
          scribe.info(s"Name: ${s("name")}, Age: ${s("age")}")
        }
        case None => scribe.info("No record found!")
      }
      Some(t.create("name" -> "Matt Hicks", "age" -> 41))
    }
    val result = Await.result(future, Duration.Inf)
    scribe.info(s"Result: $result")
    db.dispose()

    /*val map = new ConcurrentHashMap[String, Future[Unit]]()

    map.computeIfPresent("test", (key, f) => {
      ???
    })

    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(8)

    val db = HaloDB.open("testdb", opts)
    try {
      db.put("Test1".bytes, "Hello, World!".bytes)
      db.put("Test2".bytes, "Goodbye, World!".bytes)

      println(s"Result: ${db.get("Test2".bytes).string}")

      db.newIterator().asScala.foreach { record =>
        println(s"Record: ${record.getKey.string} = ${record.getValue.string}")
      }
    } finally {
      db.close()
    }*/
  }
}
