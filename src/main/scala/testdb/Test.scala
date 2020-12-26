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
    /*val t = StoredType(Vector(
      ValueTypeEntry("name", StringType),
      ValueTypeEntry("age", IntType)
    ))
    val bytes = t.create("name" -> "Hello, World", "age" -> 26)

    val stored = t(bytes)
    println(s"Name: [${stored("name")}]")
    println(s"Age: [${stored("age")}]")

    val db = new LightDB()
    val future = db.modify(Id[String]("testing", "hello")) { option =>
      scribe.info(s"Existing? ${option.map(array => new String(array, "UTF-8"))}")
      Some("Hello, World".getBytes("UTF-8"))
    }
    val result = Await.result(future, Duration.Inf)
    scribe.info(s"Result: $result")
    db.dispose()*/

    val q = new ObjectTaskQueue
    val p1 = Promise[String]
    val p2 = Promise[String]
    val f1 = q("test") {
      scribe.info("Starting execution...")
      p1.future
    }
    val f2 = q("test") {
      scribe.info("Second execution!")
      p2.future
    }
    scribe.info(s"Is Empty? ${q.isEmpty}")
    p1.success("Wahoo!")
    scribe.info(s"Is Empty? ${q.isEmpty}")
    Await.result(f1, Duration.Inf)
    scribe.info(s"Is Empty? ${q.isEmpty}")
    p2.success("Next!")
    scribe.info(s"Is Empty? ${q.isEmpty}")
    Await.result(f2, Duration.Inf)
    scribe.info(s"Is Empty? ${q.isEmpty}")

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
