package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import benchmark.imdb.MariaDBImplementation.{TitleAkaPG, fromRS}
import lightdb.Unique

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.collection.parallel.CollectionConverters._

object DerbyBench extends Bench {
  private lazy val connection: Connection = {
    val path = new File("db/derby").getCanonicalPath
    val c = DriverManager.getConnection(s"jdbc:derby:$path;create=true")
    c.setAutoCommit(false)
    c
  }

  override def name: String = "Derby"

  override def init(): Unit = {
//    executeUpdate("DROP TABLE IF EXISTS people")
    executeUpdate("CREATE TABLE people(id VARCHAR(255), name VARCHAR(255), age INTEGER)")
    executeUpdate("CREATE INDEX id_idx ON people(id)")
    executeUpdate("CREATE INDEX age_idx ON people(age)")
  }

  override protected def insertRecords(iterator: Iterator[P]): Unit = {
    val ps = connection.prepareStatement("INSERT INTO people(id, name, age) VALUES (?, ?, ?)")
    iterator.foreach { p =>
      ps.setString(1, p.id)
      ps.setString(2, p.name)
      ps.setInt(3, p.age)
      ps.addBatch()
    }
    ps.executeBatch()
    ps.close()
    connection.commit()
  }

  override protected def streamRecords(f: Iterator[P] => Unit): Unit = {
    val s = connection.createStatement()
    val rs = s.executeQuery("SELECT * FROM people")
    val iterator = rsIterator(rs)
    f(iterator)
    rs.close()
    s.close()
  }

  override protected def searchEachRecord(ageIterator: Iterator[Int]): Unit = {
    val ps = connection.prepareStatement("SELECT * FROM people WHERE age = ?")
    ageIterator.foreach { age =>
      ps.setInt(1, age)
      val rs = ps.executeQuery()
      val list = rsIterator(rs).toList
      val p = list.head
      if (p.age != age) {
        scribe.warn(s"${p.age} was not $age")
      }
      if (list.size > 1) {
        scribe.warn(s"More than one result for $age")
      }
    }
    ps.close()
  }

  override protected def searchAllRecords(f: Iterator[P] => Unit): Unit = {
    streamRecords(f)
  }

  override def size(): Long = {
    def recurse(file: File): Long = if (file.isDirectory) {
      file.listFiles().map(recurse).sum
    } else {
      file.length()
    }
    recurse(new File("db/derby"))
  }

  override def dispose(): Unit = {
    connection.commit()
    connection.close()
  }

  private def executeUpdate(sql: String): Unit = {
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } finally {
      s.close()
    }
  }

  case class Person(name: String, age: Int, id: String = Unique())
}