package lightdb.sql

import cats.effect.IO
import fabric._
import fabric.io.JsonFormatter
import lightdb.{Document, Id}
import lightdb.index.{Index, IndexSupport, Materialized}
import lightdb.model.AbstractCollection
import lightdb.query.{PagedResults, Query, SearchContext, Sort, SortDirection}
import lightdb.util.FlushingBacklog

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}
import scala.util.Try

// TODO: Move all of IndexSupport custom code into SQLIndexed
trait SQLSupport[D <: Document[D]] extends IndexSupport[D] {
  private var _connection: Option[Connection] = None

  protected def enableAutoCommit: Boolean = false

  protected[lightdb] def connection: Connection = _connection match {
    case Some(c) => c
    case None =>
      val c = createConnection()
      _connection = Some(c)
      init(c)
      c
  }

  protected def createConnection(): Connection

  protected def createTable(): String

  protected def init(c: Connection): Unit = {
    c.setAutoCommit(enableAutoCommit)
    val s = c.createStatement()
    try {
      s.executeUpdate(createTable())
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

  val _id: Index[Id[D], D] = index.one("_id", _._id, materialize = true)

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

  protected def truncateSQL: String = s"DELETE FROM ${collection.collectionName}"

  def truncate(): IO[Unit] = IO.blocking {
    val sql = truncateSQL
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
           |  COUNT(*) AS count
           |FROM
           |  ${collection.collectionName}
           |$filters
           |""".stripMargin
      val countPs = prepare(sqlCount, params)
      try {
        val rs = countPs.executeQuery()
        rs.next()
//        scribe.info(s"Columns: ${rs.getMetaData.getColumnType(1)}")
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
    val indexes = query.materializedIndexes match {
      case l if !l.contains(_id) => _id :: l
      case l => l
    }
    val fieldNames = indexes.map(_.fieldName).mkString(", ")
    val sql =
      s"""SELECT
         |  $fieldNames
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
      val materialized = this.materialized(rs, indexes)
      PagedResults(
        query = query,
        context = SQLPageContext(context),
        offset = offset,
        total = total,
        idsAndScores = materialized.map(_.apply(_id)).map(id => id -> 0.0),
        materialized = materialized,
        getter = None
      )
    } finally {
      rs.close()
      ps.close()
    }
  }

  protected def materialized(rs: ResultSet, indexes: List[Index[_, D]]): List[Materialized[D]] = {
    val iterator = new Iterator[Materialized[D]] {
      override def hasNext: Boolean = rs.next()

      override def next(): Materialized[D] = {
        val map = indexes.map { index =>
          index.fieldName -> getJson(rs, index.fieldName)
        }.toMap
        Materialized[D](Obj(map))
      }
    }
    iterator.toList
  }

  override protected def indexDoc(doc: D, fields: List[Index[_, D]]): IO[Unit] =
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

  private def getJson(rs: ResultSet, fieldName: String): Json = rs.getObject(fieldName) match {
    case s: String => str(s)
    case i: java.lang.Integer => num(i.intValue())
    case v => throw new UnsupportedOperationException(s"$fieldName returned $v (${v.getClass.getName})")
  }

  private def commit(): IO[Unit] = IO.blocking {
    if (!enableAutoCommit)
      connection.commit()
  }

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