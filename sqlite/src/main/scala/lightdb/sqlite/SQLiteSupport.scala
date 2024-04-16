package lightdb.sqlite

import cats.effect.IO
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.{Document, Id}
import lightdb.index.{IndexSupport, IndexedField}
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.util.FlushingBacklog

import java.nio.file.Path
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}

trait SQLiteSupport[D <: Document[D]] extends IndexSupport[D] {
  private lazy val path: Path = db.directory.resolve(collectionName).resolve("sqlite.db")
  // TODO: Should each collection have a connection?
  private[sqlite] lazy val connection: Connection = {
    val c = DriverManager.getConnection(s"jdbc:sqlite:${path.toFile.getCanonicalPath}")
    c.setAutoCommit(false)
    val s = c.createStatement()
    try {
      s.executeUpdate(s"CREATE TABLE IF NOT EXISTS $collectionName(${index.fields.map(_.fieldName).mkString(", ")}, PRIMARY KEY (_id))")
      index.fields.foreach { f =>
        if (f.fieldName != "_id") {
          val indexName = s"${f.fieldName}_idx"
          s.executeUpdate(s"CREATE INDEX IF NOT EXISTS $indexName ON $collectionName(${f.fieldName})")
        }
      }
    } finally {
      s.close()
    }
    c
  }

  override lazy val index: SQLiteIndexer[D] = SQLiteIndexer(this)

  val _id: SQLIndexedField[Id[D], D] = index("_id", doc => Some(doc._id))

  private lazy val backlog = new FlushingBacklog[D](10_000, 100_000) {
    override protected def write(list: List[D]): IO[Unit] = IO {
      val sql = s"INSERT OR REPLACE INTO $collectionName(${index.fields.map(_.fieldName).mkString(", ")}) VALUES (${index.fields.map(_ => "?").mkString(", ")})"
      val ps = connection.prepareStatement(sql)
      try {
        list.foreach { doc =>
          index.fields.map(_.get(doc)).zipWithIndex.foreach {
            case (value, index) => setValue(ps, index + 1, value)
          }
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  override def doSearch(query: Query[D],
                        context: SearchContext[D],
                        offset: Int,
                        after: Option[PagedResults[D]]): IO[PagedResults[D]] = IO {
    var params = List.empty[Option[Any]]
    val filters = query.filter match {
      case Some(f) =>
        val filter = f.asInstanceOf[SQLFilter[_, D]]
        params = Some(filter.value) :: params
        s"WHERE\n  ${filter.fieldName} ${filter.condition} ?"
      case None => ""
    }
    val total = if (query.countTotal) {
      val sqlCount = s"""SELECT
                        |  COUNT(*)
                        |FROM
                        |  $collectionName
                        |$filters
                        |""".stripMargin
      val countPs = prepare(sqlCount, params.reverse)
      try {
        val rs = countPs.executeQuery()
        rs.getInt(1)
      } finally {
        countPs.close()
      }
    } else {
      -1
    }
    // TODO: Add sort
    val sql = s"""SELECT
                 |  *
                 |FROM
                 |  $collectionName
                 |$filters
                 |LIMIT
                 |  ${query.pageSize}
                 |OFFSET
                 |  $offset
                 |""".stripMargin
//    scribe.info(sql)
    val ps = prepare(sql, params.reverse)
    val rs = ps.executeQuery()
    try {
      val data = this.data(rs)
      PagedResults(
        query = query,
        context = SQLPageContext(context),
        offset = offset,
        total = total,
        ids = data.ids,
        getter = data.lookup
      )
    } finally {
      rs.close()
      ps.close()
    }
  }

  protected def data(rs: ResultSet): SQLData[D] = {
    val iterator = new Iterator[Id[D]] {
      override def hasNext: Boolean = rs.next()
      override def next(): Id[D] = Id[D](rs.getString(1))
    }
    val ids = iterator.toList
    SQLData(ids, None)
  }

  override protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit] =
    backlog.enqueue(doc).map(_ => ())

  private def prepare(sql: String, params: List[Option[Any]]): PreparedStatement = {
    val ps = connection.prepareStatement(sql)
    params.zipWithIndex.foreach {
      case (value, index) => setValue(ps, index + 1, value)
    }
    ps
  }

  private def setValue(ps: PreparedStatement, index: Int, value: Option[Any]): Unit = value match {
    case Some(v) => v match {
      case s: String => ps.setString(index, s)
      case i: Int => ps.setInt(index, i)
      case b: Boolean => ps.setBoolean(index, b)
      case id: Id[_] => ps.setString(index, id.value)
      case _ => throw new RuntimeException(s"Unsupported value for $collectionName (index: $index): $value")
    }
    case None => ps.setNull(index, Types.NULL)
  }

  override def commit(): IO[Unit] = super.commit().flatMap { _ =>
    backlog.flush()
  }

  override def dispose(): IO[Unit] = super.dispose().map { _ =>
    connection.close()
  }
}

case class SQLData[D <: Document[D]](ids: List[Id[D]], lookup: Option[Id[D] => IO[D]])