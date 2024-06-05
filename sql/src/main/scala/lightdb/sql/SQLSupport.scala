package lightdb.sql

import cats.effect.IO
import fabric._
import fabric.io.JsonFormatter
import lightdb.{Document, Id}
import lightdb.index.{IndexSupport, IndexedField}
import lightdb.model.AbstractCollection
import lightdb.query.{PagedResults, Query, SearchContext, Sort, SortDirection}
import lightdb.util.FlushingBacklog

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}

trait SQLSupport[D <: Document[D]] extends IndexSupport[D] {
  private var _connection: Option[Connection] = None

  protected[lightdb] def connection: Connection = _connection match {
    case Some(c) => c
    case None =>
      val c = createConnection()
      _connection = Some(c)
      init(c)
      c
  }

  protected def createConnection(): Connection

  protected def init(c: Connection): Unit = {
    c.setAutoCommit(false)
    val s = c.createStatement()
    try {
      s.executeUpdate(s"CREATE TABLE IF NOT EXISTS ${collection.collectionName}(${index.fields.map(_.fieldName).mkString(", ")}, PRIMARY KEY (_id))")
      val existingColumns = columns(c)
      index.fields.foreach { f =>
        if (f.fieldName != "_id") {
          if (!existingColumns.contains(f.fieldName)) {
            s.executeUpdate(s"ALTER TABLE ${collection.collectionName} ADD ${f.fieldName}")
          }
          val indexName = s"${f.fieldName}_idx"
          s.executeUpdate(s"CREATE INDEX IF NOT EXISTS $indexName ON ${collection.collectionName}(${f.fieldName})")
        }
      }
    } finally {
      s.close()
    }
  }

  def columns(connection: Connection = connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM ${collection.collectionName} LIMIT 1")
    try {
      val rs = ps.executeQuery()
      val meta = rs.getMetaData
      (1 to meta.getColumnCount).map { index =>
        meta.getColumnName(index)
      }.toSet
    } finally {
      ps.close()
    }
  }

  override lazy val index: SQLIndexer[D] = SQLIndexer(this)

  val _id: SQLIndexedField[Id[D], D] = index.one("_id", _._id)

  private[lightdb] lazy val backlog = new FlushingBacklog[Id[D], D](1_000, 10_000) {
    override protected def write(list: List[D]): IO[Unit] = IO.blocking {
      val sql = s"INSERT OR REPLACE INTO ${collection.collectionName}(${index.fields.map(_.fieldName).mkString(", ")}) VALUES (${index.fields.map(_ => "?").mkString(", ")})"
      val ps = connection.prepareStatement(sql)
      try {
        list.foreach { doc =>
          index.fields.map(_.getJson(doc)).zipWithIndex.foreach {
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

  def truncate(): IO[Unit] = IO.blocking {
    val sql = s"DELETE FROM ${collection.collectionName}"
    val ps = connection.prepareStatement(sql)
    try {
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def doSearch[V](query: Query[D, V],
                           context: SearchContext[D],
                           offset: Int,
                           limit: Option[Int],
                           after: Option[PagedResults[D, V]]): IO[PagedResults[D, V]] = IO.blocking {
    var params = List.empty[Json]
    val filters = query.filter match {
      case Some(f) =>
        val filter = f.asInstanceOf[SQLPart]
        params = params ::: filter.args
        s"WHERE\n  ${filter.sql}"
      case None => ""
    }
    val total = if (query.countTotal) {
      val sqlCount =
        s"""SELECT
           |  COUNT(*)
           |FROM
           |  ${collection.collectionName}
           |$filters
           |""".stripMargin
      val countPs = prepare(sqlCount, params)
      try {
        val rs = countPs.executeQuery()
        rs.getInt(1)
      } finally {
        countPs.close()
      }
    } else {
      -1
    }
    val sort = query.sort.collect {
      case Sort.ByField(field, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        s"${field.fieldName} $dir"
    } match {
      case Nil => ""
      case list => list.mkString("ORDER BY ", ", ", "")
    }
    val sql =
      s"""SELECT
         |  *
         |FROM
         |  ${collection.collectionName}
         |$filters
         |$sort
         |LIMIT
         |  ${query.limit.getOrElse(query.pageSize)}
         |OFFSET
         |  $offset
         |""".stripMargin
    val ps = prepare(sql, params)
    val rs = ps.executeQuery()
    try {
      val data = this.data(rs)
      PagedResults(
        query = query,
        context = SQLPageContext(context),
        offset = offset,
        total = total,
        idsAndScores = data.ids.map(id => id -> 0.0),
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

      override def next(): Id[D] = Id[D](rs.getString("_id"))
    }
    val ids = iterator.toList
    SQLData(ids, None)
  }

  override protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit] =
    backlog.enqueue(doc._id, doc).map(_ => ())

  private def prepare(sql: String, params: List[Json]): PreparedStatement = try {
    val ps = connection.prepareStatement(sql)
    params.zipWithIndex.foreach {
      case (value, index) => setValue(ps, index + 1, value)
    }
    ps
  } catch {
    case t: Throwable => throw new RuntimeException(s"Error handling SQL query: $sql (params: ${params.mkString(", ")})", t)
  }

  private def setValue(ps: PreparedStatement, index: Int, value: Json): Unit = value match {
    case Null => ps.setNull(index, Types.NULL)
    case Str(s, _) => ps.setString(index, s)
    case Bool(b, _) => ps.setBoolean(index, b)
    case NumInt(l, _) => ps.setLong(index, l)
    case NumDec(bd, _) => ps.setBigDecimal(index, bd.bigDecimal)
    case Arr(v, _) => v.toList match {
      case Nil => ps.setNull(index, Types.NULL)
      case value :: Nil => setValue(ps, index, value)
      case _ => throw new RuntimeException(s"SQLite does not support more than one element in an array ($value)")
    }
    case _ => ps.setString(index, JsonFormatter.Compact(value))
  }

  private def commit(): IO[Unit] = IO.blocking(connection.commit())

  override protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    super.initModel(collection)
    collection.commitActions.add(backlog.flush())
    collection.commitActions.add(commit())
    collection.truncateActions.add(truncate())
    collection.disposeActions.add(IO.blocking {
      connection.close()
      _connection = None
    })
  }
}