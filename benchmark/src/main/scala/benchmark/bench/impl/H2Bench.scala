package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import lightdb.util.Unique

import java.io.File
import java.sql.{Connection, DriverManager}
import scala.collection.parallel.CollectionConverters._

object H2Bench extends Bench {
  private lazy val connection: Connection = {
    val path = new File("db/h2").getCanonicalPath
    val c = DriverManager.getConnection(s"jdbc:h2:$path")
    c.setAutoCommit(false)
    c
  }

  override def name: String = "H2"

  override def init(): Unit = {
    executeUpdate("DROP TABLE IF EXISTS people")
    executeUpdate("CREATE TABLE people(id VARCHAR NOT NULL, name TEXT, age INTEGER, PRIMARY KEY (id))")
    executeUpdate("CREATE INDEX age_idx ON people(age)")
  }

  override protected def insertRecords(status: StatusCallback): Int = {
    val ps = connection.prepareStatement("INSERT INTO people(id, name, age) VALUES (?, ?, ?)")
    try {
      (0 until RecordCount)
        .foldLeft(0)((total, index) => {
          val person = Person(
            name = Unique(),
            age = index
          )
          ps.setString(1, person.id)
          ps.setString(2, person.name)
          ps.setInt(3, person.age)
          ps.addBatch()
          status.progress.set(index + 1)
          total + 1
        })
    } finally {
      ps.executeBatch()
      ps.close()
      connection.commit()
    }
  }

  private def countRecords(): Int = {
    val s = connection.createStatement()
    try {
      val rs = s.executeQuery("SELECT COUNT(*) FROM people")
      try {
        rs.next()
        rs.getInt(1)
      } finally {
        rs.close()
      }
    } finally {
      s.close()
    }
  }

  override protected def streamRecords(status: StatusCallback): Int = (0 until StreamIterations)
    .foldLeft(0)((total, iteration) => {
      val s = connection.createStatement()
      val rs = s.executeQuery("SELECT * FROM people")
      var count = 0
      while (rs.next()) {
        count += 1
      }
      rs.close()
      s.close()
      if (count != RecordCount) {
        scribe.warn(s"RecordCount was not $RecordCount, it was $count")
      }
      status.progress.set(iteration + 1)
      total + count
    })

  override protected def searchEachRecord(status: StatusCallback): Int = {
    var counter = 0
    (0 until StreamIterations)
      .foreach { iteration =>
        val ps = connection.prepareStatement("SELECT * FROM people WHERE age = ?")
        (0 until RecordCount)
          .foreach { index =>
            ps.setInt(1, index)
            val rs = ps.executeQuery()
            rs.next()
            val person = Person(
              name = rs.getString("name"),
              age = rs.getInt("age"),
              id = rs.getString("id")
            )
            if (person.age != index) {
              scribe.warn(s"${person.age} was not $index")
            }
            if (rs.next()) {
              scribe.warn(s"More than one result for $index")
            }
            rs.close()
            counter += 1
            status.progress.set((iteration + 1) * (index + 1))
          }
        ps.close()
      }
    counter
  }

  override protected def searchAllRecords(status: StatusCallback): Int = {
    var counter = 0
    (0 until StreamIterations)
      .par
      .foreach { iteration =>
        val s = connection.createStatement()
        val rs = s.executeQuery("SELECT * FROM people")
        var count = 0
        while (rs.next()) {
          count += 1
          counter += 1
        }
        rs.close()
        s.close()
        if (count != RecordCount) {
          scribe.warn(s"RecordCount was not $RecordCount, it was $count")
        }
        status.progress.set(iteration + 1)
      }
    counter
  }

  override def size(): Long = new File("db/sqlite.db").length()

  override def dispose(): Unit = connection.close()

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