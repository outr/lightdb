package benchmark.imdb

import benchmark.FlushingBacklog
import cats.effect.unsafe.IORuntime
import rapid.Task

import java.sql.{Connection, DriverManager, ResultSet}

object MariaDBImplementation extends BenchmarkImplementation {
  implicit val runtime: IORuntime = IORuntime.global

  override type TitleAka = TitleAkaPG
  override type TitleBasics = TitleBasicsPG

  private lazy val connection: Connection = {
    val c = DriverManager.getConnection("jdbc:mariadb://localhost:3306/imdb", "root", "password")
    c.setAutoCommit(false)
    c
  }

  private lazy val backlogAka = new FlushingBacklog[String, TitleAka](1000, 10000) {
    override protected def write(list: List[TitleAkaPG]): Task[Unit] = Task {
      val ps = connection.prepareStatement("INSERT INTO title_aka(id, titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
      try {
        list.foreach { t =>
          ps.setString(1, t.id)
          ps.setString(2, t.titleId)
          ps.setInt(3, t.ordering)
          ps.setString(4, t.title)
          ps.setString(5, t.region)
          ps.setString(6, t.language)
          ps.setString(7, t.types)
          ps.setString(8, t.attributes)
          ps.setInt(9, t.isOriginalTitle)
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  private lazy val backlogBasics = new FlushingBacklog[String, TitleBasics](1000, 10000) {
    override protected def write(list: List[TitleBasicsPG]): Task[Unit] = Task {
      val ps = connection.prepareStatement("INSERT INTO title_basics(id, tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      try {
        list.foreach { t =>
          ps.setString(1, t.id)
          ps.setString(2, t.tconst)
          ps.setString(3, t.titleType)
          ps.setString(4, t.primaryTitle)
          ps.setString(5, t.originalTitle)
          ps.setInt(6, t.isAdult)
          ps.setInt(7, t.startYear)
          ps.setInt(8, t.endYear)
          ps.setInt(9, t.runtimeMinutes)
          ps.setString(10, t.genres)
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  override def name: String = "MariaDB"

  override def init(): Task[Unit] = Task {
    executeUpdate("DROP TABLE IF EXISTS title_aka")
    executeUpdate("DROP TABLE IF EXISTS title_basics")
    executeUpdate("CREATE TABLE title_aka(id VARCHAR(128) NOT NULL, titleId TEXT, ordering INTEGER, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, isOriginalTitle SMALLINT, PRIMARY KEY (id))")
    executeUpdate("CREATE TABLE title_basics(id VARCHAR(128) NOT NULL, tconst TEXT, titleType TEXT, primaryTitle TEXT, originalTitle TEXT, isAdult INTEGER, startYear INTEGER, endYear INTEGER, runtimeMinutes INTEGER, genres TEXT, PRIMARY KEY (id))")
    executeUpdate("CREATE INDEX title_aka_title_id_idx ON title_aka(titleId)")
  }

  override def map2TitleAka(map: Map[String, String]): TitleAka = TitleAkaPG(
    id = map.option("id").getOrElse(rapid.Unique()),
    titleId = map.value("titleId"),
    ordering = map.int("ordering"),
    title = map.value("title"),
    region = map.option("region").getOrElse(""),
    language = map.option("language").getOrElse(""),
    types = map.option("types").getOrElse(""),
    attributes = map.option("attributes").getOrElse(""),
    isOriginalTitle = map.boolOption("isOriginalTitle").map(b => if (b) 1 else 0).getOrElse(-1)
  )

  override def map2TitleBasics(map: Map[String, String]): TitleBasicsPG = TitleBasicsPG(
    id = map.option("id").getOrElse(rapid.Unique()),
    tconst = map.value("tconst"),
    titleType = map.value("titleType"),
    primaryTitle = map.value("primaryTitle"),
    originalTitle = map.value("originalTitle"),
    isAdult = map.int("isAdult"),
    startYear = map.int("startYear"),
    endYear = map.int("endYear"),
    runtimeMinutes = map.int("runtimeMinutes"),
    genres = map.value("genres")
  )

  override def persistTitleAka(t: TitleAka): Task[Unit] = backlogAka.enqueue(t.id, t).map(_ => ())

  override def persistTitleBasics(t: TitleBasicsPG): Task[Unit] = backlogBasics.enqueue(t.id, t).map(_ => ())

  private def fromRS(rs: ResultSet): TitleAkaPG = TitleAkaPG(
    id = rs.getString("id"),
    titleId = rs.getString("titleId"),
    ordering = rs.getInt("ordering"),
    title = rs.getString("title"),
    region = rs.getString("region"),
    language = rs.getString("language"),
    types = rs.getString("types"),
    attributes = rs.getString("attributes"),
    isOriginalTitle = rs.getInt("isOriginalTitle")
  )

  override def streamTitleAka(): rapid.Stream[TitleAkaPG] = {
    val s = connection.createStatement()
    try {
      val rs = s.executeQuery("SELECT * FROM title_aka")
      val iterator = Iterator.unfold(rs) { rs =>
        if (rs.next()) {
          Some(fromRS(rs) -> rs)
        } else {
          None
        }
      }
      rapid.Stream.fromIterator(Task(iterator))
    } finally {
      s.closeOnCompletion()
    }
  }

  override def idFor(t: TitleAkaPG): String = t.id

  override def titleIdFor(t: TitleAkaPG): String = t.titleId

  override def get(id: String): Task[TitleAkaPG] = Task {
    val s = connection.prepareStatement("SELECT * FROM title_aka WHERE id = ?")
    try {
      s.setString(1, id)
      val rs = s.executeQuery()
      try {
        rs.next()
        fromRS(rs)
      } finally {
        rs.close()
      }
    } finally {
      s.close()
    }
  }

  override def findByTitleId(titleId: String): Task[List[TitleAkaPG]] = Task {
    val s = connection.prepareStatement("SELECT * FROM title_aka WHERE titleId = ?")
    try {
      s.setString(1, titleId)
      val rs = s.executeQuery()
      try {
        new Iterator[TitleAkaPG] {
          override def hasNext: Boolean = rs.next()

          override def next(): TitleAkaPG = fromRS(rs)
        }.toList
      } finally {
        rs.close()
      }
    } finally {
      s.close()
    }
  }

  override def flush(): Task[Unit] = for {
    _ <- backlogAka.flush()
    _ <- Task(commit())
  } yield {
    ()
  }

  override def verifyTitleAka(): Task[Unit] = Task {
    val s = connection.createStatement()
    val rs = s.executeQuery("SELECT COUNT(1) FROM title_aka")
    rs.next()
    val count = rs.getInt(1)
    scribe.info(s"Counted $count records in title_aka table")
  }

  override def verifyTitleBasics(): Task[Unit] = Task {
    val s = connection.createStatement()
    val rs = s.executeQuery("SELECT COUNT(1) FROM title_basics")
    rs.next()
    val count = rs.getInt(1)
    scribe.info(s"Counted $count records in title_basics table")
  }

  private def executeUpdate(sql: String): Unit = {
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } finally {
      s.close()
    }
  }

  private def commit(): Unit = connection.commit()

  case class TitleAkaPG(id: String, titleId: String, ordering: Int, title: String, region: String, language: String, types: String, attributes: String, isOriginalTitle: Int)
  case class TitleBasicsPG(id: String, tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Int, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: String)
}
