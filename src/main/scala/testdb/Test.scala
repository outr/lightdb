package testdb

import java.io.File
import com.oath.halodb.{HaloDB, HaloDBOptions}
import io.circe.{Json, Printer}
import io.youi.Unique

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

object Test {
  def main(args: Array[String]): Unit = {
    val t = StoredType(Vector(
      ValueTypeEntry("name", StringType),
      ValueTypeEntry("age", IntType)
    ))
    val bytes = t.create("name" -> "Hello, World", "age" -> 26)

    val stored = t(bytes)
    println(s"Name: [${stored("name")}]")
    println(s"Age: [${stored("age")}]")

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
