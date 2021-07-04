package benchmark
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import lightdb.util.FlushingBacklog

import java.sql.{Connection, DriverManager}
import scala.concurrent.{ExecutionContext, Future}

object PostgresImplementation extends BenchmarkImplementation {
  implicit val runtime: IORuntime = IORuntime.global

  override type TitleAka = TitleAkaPG

  private lazy val connection: Connection = {
    val c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/imdb", "postgres", "password")
    c.setAutoCommit(false)
    c
  }

  private lazy val backlog = new FlushingBacklog[TitleAka](1000, 10000) {
    override protected def write(list: List[TitleAkaPG]): IO[Unit] = IO {
      val ps = connection.prepareStatement("INSERT INTO title_aka(titleId, ordering, title, region, language, types, attributes, isOriginalTitle) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
      try {
        list.foreach { t =>
          ps.setString(1, t.titleId)
          ps.setInt(2, t.ordering)
          ps.setString(3, t.title)
          ps.setString(4, t.region)
          ps.setString(5, t.language)
          ps.setString(6, t.types)
          ps.setString(7, t.attributes)
          ps.setInt(8, t.isOriginalTitle)
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  override def name: String = "PostgreSQL"

  override def init()(implicit ec: ExecutionContext): Future[Unit] = Future {
    executeUpdate("DROP TABLE IF EXISTS title_aka")
    executeUpdate("CREATE TABLE title_aka(id SERIAL PRIMARY KEY, titleId TEXT, ordering INTEGER, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, isOriginalTitle SMALLINT)")
  }

  override def map2TitleAka(map: Map[String, String]): TitleAka = TitleAkaPG(
    titleId = map.value("titleId"),
    ordering = map.int("ordering"),
    title = map.value("title"),
    region = map.option("region").getOrElse(""),
    language = map.option("language").getOrElse(""),
    types = map.option("types").getOrElse(""),
    attributes = map.option("attributes").getOrElse(""),
    isOriginalTitle = map.boolOption("isOriginalTitle").map(b => if (b) 1 else 0).getOrElse(-1)
  )

  override def persistTitleAka(t: TitleAka)(implicit ec: ExecutionContext): Future[Unit] = backlog.enqueue(t).unsafeToFuture().map(_ => ())

  override def flush()(implicit ec: ExecutionContext): Future[Unit] = for {
    _ <- backlog.flush().unsafeToFuture()
    _ <- Future(commit())
  } yield {
    ()
  }

  override def verifyTitleAka()(implicit ec: ExecutionContext): Future[Unit] = Future {
    val s = connection.createStatement()
    val rs = s.executeQuery("SELECT COUNT(1) FROM title_aka")
    rs.next()
    val count = rs.getInt(1)
    scribe.info(s"Counted $count records in title_aka table")
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

  case class TitleAkaPG(titleId: String, ordering: Int, title: String, region: String, language: String, types: String, attributes: String, isOriginalTitle: Int)
}
