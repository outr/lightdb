package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import fabric.define.DefType
import fabric.rw.{Asable, Convertible, RW}
import fabric.{Json, NumInt, Obj, Str}
import lightdb.util.Unique

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.parallel.CollectionConverters._

object SQLiteTweaked2Bench extends Bench {
  private lazy val connection: Connection = {
    val c = DriverManager.getConnection("jdbc:sqlite:db/sqlite-tweaked2.db")
    c.setAutoCommit(false)
    c
  }

  override def name: String = "SQLite-Tweaked2"

  override def init(): Unit = {
    executeUpdate("DROP TABLE IF EXISTS people")
    executeUpdate("CREATE TABLE people(id VARCHAR NOT NULL, name TEXT, age INTEGER, PRIMARY KEY (id))")
    executeUpdate("CREATE INDEX age_idx ON people(age)")
  }

  override protected def insertRecords(status: StatusCallback): Int = {
    var batchSize = 0
    val obj = Person.rw.definition.asInstanceOf[DefType.Obj]
    val ats = obj.map.keys.toList
    val ps = connection.prepareStatement(s"INSERT OR REPLACE INTO people(${ats.mkString(", ")}) VALUES (${ats.map(_ => "?").mkString(", ")})")
    try {
      (0 until RecordCount)
        .foldLeft(0)((total, index) => {
          val person = Person(
            name = Unique(),
            age = index
          )
          val json = person.json
          ats.zipWithIndex.foreach {
            case (key, index) => json(key) match {
              case Str(s, _) => ps.setString(index + 1, s)
              case NumInt(l, _) => ps.setLong(index + 1, l)
              case j => throw new RuntimeException(s"Unsupported JSON: $j for $key")
            }
          }
          ps.addBatch()
          batchSize += 1
          if (batchSize > 1_000_000) {
            ps.executeBatch()
            batchSize = 0
          }
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
        val person = personBuilder(rs)
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
            val person = personBuilder(rs)
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

  private val personBuilder = {
    val d = Person.rw.definition.asInstanceOf[DefType.Obj]
    def t2Json(key: String, dt: DefType): ResultSet => (String, Json) = dt match {
      case DefType.Str => rs => key -> Str(rs.getString(key))
      case DefType.Int => rs => key -> NumInt(rs.getLong(key))
      case DefType.Opt(dt) => t2Json(key, dt)
      case _ => throw new RuntimeException(s"Unsupported DefType: $dt")
    }
    val list = d.map.toList.map {
      case (key, dt) => t2Json(key, dt)
    }
    (rs: ResultSet) => {
      val json = Obj(list.map(_(rs)): _*)
      json.as[Person]
    }
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
          val person = personBuilder(rs)

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

  override def size(): Long = new File("db/sqlite-tweaked.db").length()

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

  object Person {
    implicit val rw: RW[Person] = RW.gen
  }
}