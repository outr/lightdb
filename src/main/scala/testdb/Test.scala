package testdb

import com.oath.halodb.{HaloDB, HaloDBOptions}

import scala.jdk.CollectionConverters._

object Test {
  def main(args: Array[String]): Unit = {
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
    }
  }

  implicit class BytesExtras(val array: Array[Byte]) extends AnyVal {
    def string: String = new String(array, "UTF-8")
  }

  implicit class StringExtras(val s: String) extends AnyVal {
    def bytes: Array[Byte] = s.getBytes("UTF-8")
  }
}