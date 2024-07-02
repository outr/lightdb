package benchmark.bench.impl

import benchmark.bench.{Bench, StatusCallback}
import fabric.define.DefType
import fabric.rw.{Asable, Convertible, RW}
import fabric.{Json, NumInt, Obj, Str}
import lightdb.util.Unique
import org.jooq.{DataType, SQLDialect}

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.parallel.CollectionConverters._
import org.jooq.impl._

import scala.jdk.CollectionConverters._

object SQLiteJOOQBench extends Bench {
  private lazy val fileName = "db/sqlite-jooq.db"

  private lazy val connection: Connection = {
    val c = DriverManager.getConnection(s"jdbc:sqlite:$fileName")
    c.setAutoCommit(false)
    c
  }

  private lazy val dsl = DSL.using(connection, SQLDialect.SQLITE)

  private lazy val table = DSL.table("people")

  override def name: String = "SQLite-JOOQ"

  override def init(): Unit = {
    dsl.dropTableIfExists(table).execute()
    dsl.createTableIfNotExists(table)
      .columns("id", "name", "age")
      .primaryKey("id")
      .execute()
    dsl.createIndexIfNotExists("age_idx")
      .on("people", "age")
      .execute()
  }

  override protected def insertRecords(status: StatusCallback): Int = {
    var batchSize = 0
    var batch = List.empty[Person]
    val obj = Person.rw.definition.asInstanceOf[DefType.Obj]
    val ats = obj.map.keys.toList
    val columns = ats.map(table.field)

    def flush(): Unit = {
      dsl.batch(batch.map { p =>
        val json = p.json

        val map = ats.map { key =>
          json(key) match {
            case Str(s, _) => key -> s
            case NumInt(l, _) => key -> l
            case j => throw new RuntimeException(s"Unsupported JSON: $j for $key")
          }
        }.toMap
        dsl
          .insertInto(table)
          .set(map.asJava)
      }: _*).execute()
      batchSize = 0
      batch = Nil
    }

//    val ps = connection.prepareStatement(s"INSERT OR REPLACE INTO people(${ats.mkString(", ")}) VALUES (${ats.map(_ => "?").mkString(", ")})")
    try {
      (0 until RecordCount)
        .foldLeft(0)((total, index) => {
          val person = Person(
            name = Unique(),
            age = index
          )
          batch = person :: batch
          batchSize += 1
          if (batchSize >= 100_000) {
            flush()
          }
          status.progress.set(index + 1)
          total + 1
        })
    } finally {
      flush()
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
      var count = 0
      val results = dsl.select().from(table).fetch()
      results.asScala.foreach { record =>
        val person = Person(
          name = record.get("name").asInstanceOf[String],
          age = record.get("age").asInstanceOf[java.lang.Integer].intValue(),
          id = record.get("id").asInstanceOf[String]
        )
        count += 1
      }
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

  override def size(): Long = new File(fileName).length()

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