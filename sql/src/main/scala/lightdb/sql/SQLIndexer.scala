package lightdb.sql

import cats.effect.IO
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Obj, Str, bool, num, obj, str}
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Convertible
import lightdb.Id
import lightdb.aggregate.{AggregateFilter, AggregateQuery, AggregateType}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.index.{Index, Indexed, Indexer, MaterializedAggregate, MaterializedIndex}
import lightdb.query.{Query, SearchResults, Sort, SortDirection}
import lightdb.transaction.{Transaction, TransactionKey}

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import java.util.concurrent.atomic.AtomicInteger

trait SQLIndexer[D <: Document[D], M <: DocumentModel[D]] extends Indexer[D, M] {
  private lazy val connectionKey: TransactionKey[Connection] = TransactionKey("sqlConnection")
  private lazy val insertsKey: TransactionKey[SQLInserts[D]] = TransactionKey("inserts")

  private lazy val dataSource: HikariDataSource = {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcUrl)
    username.foreach(config.setUsername)
    password.foreach(config.setPassword)
    config.setAutoCommit(false)
    new HikariDataSource(config)
  }

  protected def jdbcUrl: String
  protected def username: Option[String] = None
  protected def password: Option[String] = None

  protected def createTable(): String = {
    val entries = indexes.map { index =>
      if (index.name == "_id") {
        "_id VARCHAR PRIMARY KEY"
      } else {
        val t = index.rw.definition match {
          case DefType.Str => "VARCHAR"
          case DefType.Int => "INTEGER"
          case d => throw new UnsupportedOperationException(s"${index.name} has an unsupported type: $d")
        }
        s"${index.name} $t"
      }
    }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS ${collection.name}($entries)"
  }

  override def init(collection: Collection[D, _]): IO[Unit] = super.init(collection).flatMap { _ =>
    collection.transaction { implicit transaction =>
      IO.blocking {
        val connection = getConnection
        val statement = connection.createStatement()
        try {
          // Create the table if it doesn't exist
          statement.executeUpdate(createTable())

          // Create columns and indexes
          val existingColumns = columns(connection)
          indexes.foreach { index =>
            if (index.name != "_id") {
              if (!existingColumns.contains(index.name)) {
                statement.executeUpdate(s"ALTER TABLE ${collection.name} ADD ${index.name}")
              }
              val indexName = s"${index.name}_idx"
              statement.executeUpdate(s"CREATE INDEX IF NOT EXISTS $indexName ON ${collection.name}(${index.name})")
            }
          }
        } finally {
          statement.close()
        }
      }
    }
  }

  private def columns(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM ${collection.name} LIMIT 1")
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

  private def getConnection(implicit transaction: Transaction[D]): Connection = transaction
    .getOrCreate(connectionKey, dataSource.getConnection)

  override def transactionEnd(transaction: Transaction[D]): IO[Unit] = super.transactionEnd(transaction).map { _ =>
    transaction.get(insertsKey).foreach { inserts =>
      inserts.close()
    }
    transaction.get(connectionKey).foreach { connection =>
      connection.close()
    }
  }

  override def commit(transaction: Transaction[D]): IO[Unit] = super.commit(transaction).map { _ =>
    transaction.get(insertsKey).foreach { inserts =>
      inserts.execute()
    }
    transaction.get(connectionKey).foreach { connection =>
      connection.commit()
    }
  }

  override def postSet(doc: D, transaction: Transaction[D]): IO[Unit] = super.postSet(doc, transaction).flatMap { _ =>
    indexDoc(doc)(transaction)
  }

  override def postDelete(doc: D, transaction: Transaction[D]): IO[Unit] = super.postDelete(doc, transaction).flatMap { _ =>
    IO.blocking {
      val connection = getConnection(transaction)
      val ps = connection.prepareStatement("DELETE FROM ${collection.name} WHERE _id = ?")
      try {
        ps.setString(1, doc._id.value)
        ps.executeUpdate()
      } finally {
        ps.close()
      }
    }
  }

  override def truncate(transaction: Transaction[D]): IO[Unit] = super.truncate(transaction).flatMap { _ =>
    IO.blocking {
      val connection = getConnection(transaction)
      val statement = connection.createStatement()
      try {
        statement.executeUpdate(s"DELETE FROM ${collection.name}")
      } finally {
        statement.close()
      }
    }
  }

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking {
    val connection = getConnection
    val statement = connection.createStatement()
    try {
      val rs = statement.executeQuery(s"SELECT COUNT(_id) FROM ${collection.name}")
      try {
        rs.next()
        rs.getInt(1)
      } finally {
        rs.close()
      }
    } finally {
      statement.close()
    }
  }

  private def indexDoc(doc: D)(implicit transaction: Transaction[D]): IO[Unit] = IO.blocking {
    val connection = getConnection
    val inserts = transaction.getOrCreate(insertsKey, {
      val sql = s"INSERT OR REPLACE INTO ${collection.name}(${indexes.map(_.name).mkString(", ")}) VALUES (${indexes.map(_ => "?").mkString(", ")})"
      val ps = connection.prepareStatement(sql)
      // TODO: Make this configurable
      val maxBatch = 10_000
      new SQLInserts[D](ps, indexes, maxBatch)
    })
    inserts.insert(doc)
  }

  override def aggregate(query: AggregateQuery[D, M])
                        (implicit transaction: Transaction[D]): fs2.Stream[IO, MaterializedAggregate[D, M]] = fs2.Stream.force(IO.blocking {
    val connection = getConnection(transaction)
    val fields = query.functions.map { f =>
      val af = f.`type` match {
        case AggregateType.Max => Some("MAX")
        case AggregateType.Min => Some("MIN")
        case AggregateType.Avg => Some("AVG")
        case AggregateType.Sum => Some("SUM")
        case AggregateType.Count | AggregateType.CountDistinct => Some("COUNT")
        case AggregateType.Concat | AggregateType.ConcatDistinct => Some("GROUP_CONCAT")
        case AggregateType.Group => None
      }
      val fieldName = af match {
        case Some(s) =>
          val pre = f.`type` match {
            case AggregateType.CountDistinct | AggregateType.ConcatDistinct => "DISTINCT "
            case _ => ""
          }
          val post = f.`type` match {
            case AggregateType.Concat => ", ';;'"
            case _ => ""
          }
          s"$s($pre${f.index.name}$post)"
        case None => f.index.name
      }
      SQLPart(s"$fieldName AS ${f.name}", Nil)
    }
    val filters = query.query.filter.map(filter2Part).toList
    val group = query.functions.filter(_.`type` == AggregateType.Group).map(_.index.name).distinct.map(s => SQLPart(s, Nil))
    val having = query.filter.map(af2Part).toList
    val sort = (query.sort ::: query.query.sort).collect {
      case Sort.ByIndex(index, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart(s"${index.name} $dir", Nil)
    }
    val b = SQLQueryBuilder(
      collection = collection,
      fields = fields,
      filters = filters,
      group = group,
      having = having,
      sort = sort,
      limit = query.query.limit,
      offset = query.query.offset
    )
    val rs = b.execute(connection)
    def createStream[R](f: ResultSet => R): fs2.Stream[IO, R] = {
      val iterator = new Iterator[R] {
        private var checkedNext = false
        private var nextValue = false

        override def hasNext: Boolean = {
          if (!checkedNext) {
            nextValue = rs.next()
            checkedNext = true
          }
          nextValue
        }

        override def next(): R = {
          if (!checkedNext) {
            rs.next()
          }
          checkedNext = false
          f(rs)
        }
      }
      fs2.Stream.fromBlockingIterator[IO](iterator, query.query.batchSize)
    }
    createStream[MaterializedAggregate[D, M]] { rs =>
      val json = obj(indexes.map { index =>
        index.name -> getJson(rs, index.name)
      }: _*)
      MaterializedAggregate[D, M](json, collection.model)
    }
  })

  override def doSearch[V](query: Query[D, M],
                           transaction: Transaction[D],
                           conversion: Conversion[V]): IO[SearchResults[D, V]] = IO.blocking {
    val connection = getConnection(transaction)
    val fields = conversion match {
      case Conversion.Id => List(SQLPart("_id", Nil))
      case Conversion.Doc => List(SQLPart("_id", Nil))
      case Conversion.Materialized(indexes) => indexes.map(_.name).map(n => SQLPart(n, Nil))
      case Conversion.Distance(_, _, _, _) => throw new UnsupportedOperationException(s"Distance conversion not currently supported for SQL!")
    }
    val filters = query.filter.map(filter2Part).toList
    val sort = query.sort.collect {
      case Sort.ByIndex(index, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart(s"${index.name} $dir", Nil)
    }
    val b = SQLQueryBuilder(
      collection = collection,
      fields = fields,
      filters = filters,
      sort = sort,
      limit = query.limit,
      offset = query.offset
    )
    val total = if (query.countTotal) {
      Some(b.queryTotal(connection))
    } else {
      None
    }
    val rs = b.execute(connection)
    def createStream[R](f: ResultSet => R): fs2.Stream[IO, R] = {
      val iterator = new Iterator[R] {
        private var checkedNext = false
        private var nextValue = false

        override def hasNext: Boolean = {
          if (!checkedNext) {
            nextValue = rs.next()
            checkedNext = true
          }
          nextValue
        }

        override def next(): R = {
          if (!checkedNext) {
            rs.next()
          }
          checkedNext = false
          f(rs)
        }
      }
      fs2.Stream.fromBlockingIterator[IO](iterator, query.batchSize)
    }
    def idStream: fs2.Stream[IO, Id[D]] = createStream(rs => Id[D](rs.getString("_id")))
    val stream: fs2.Stream[IO, V] = conversion match {
      case Conversion.Id => idStream
      case Conversion.Doc => idStream.evalMap { id =>
        collection(id)(transaction)
      }
      case Conversion.Materialized(indexes) => createStream[MaterializedIndex[D, M]] { rs =>
        val json = obj(indexes.map { index =>
          index.name -> getJson(rs, index.name)
        }: _*)
        MaterializedIndex[D, M](json, collection.model)
      }
      case Conversion.Distance(_, _, _, _) => throw new UnsupportedOperationException(s"Distance conversion not currently supported for SQL!")
    }
    SearchResults(
      offset = query.offset,
      limit = query.limit,
      total = total,
      scoredStream = stream.map(v => (v, 0.0)),
      transaction = transaction
    )
  }

  private def getJson(rs: ResultSet, fieldName: String): Json = rs.getObject(fieldName) match {
    case s: String => str(s)
    case b: java.lang.Boolean => bool(b.booleanValue())
    case i: java.lang.Integer => num(i.intValue())
    case l: java.lang.Long => num(l.longValue())
    case f: java.lang.Float => num(f.doubleValue())
    case d: java.lang.Double => num(d.doubleValue())
    case null => Null
    case v => throw new UnsupportedOperationException(s"$fieldName returned $v (${v.getClass.getName})")
  }

  private def filter2Part(f: Filter[_]): SQLPart = f match {
    case f: Filter.Equals[_, _] => SQLPart(s"${f.index.name} = ?", List(f.getJson))
    case f: Filter.In[_, _] => SQLPart(s"${f.index.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.getJson)
    case f: Filter.Combined[_] =>
      val parts = f.filters.map(f => filter2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: Filter.RangeLong[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.index.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.index.name} < ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} > ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.index.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.index.name} < ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} > ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.Parsed[_, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: Filter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }

  private def af2Part(f: AggregateFilter[_]): SQLPart = f match {
    case f: AggregateFilter.Equals[_, _] => SQLPart(s"${f.index.name} = ?", List(f.getJson))
    case f: AggregateFilter.In[_, _] => SQLPart(s"${f.index.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.getJson)
    case f: AggregateFilter.Combined[_] =>
      val parts = f.filters.map(f => af2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: AggregateFilter.RangeLong[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.index.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.index.name} < ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} > ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.index.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.index.name} < ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} > ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.Parsed[_, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: AggregateFilter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }
}

case class SQLQueryBuilder[D <: Document[D]](collection: Collection[D, _],
                                             fields: List[SQLPart] = Nil,
                                             filters: List[SQLPart] = Nil,
                                             group: List[SQLPart] = Nil,
                                             having: List[SQLPart] = Nil,
                                             sort: List[SQLPart] = Nil,
                                             limit: Option[Int] = None,
                                             offset: Int) {
  def queryTotal(connection: Connection): Int = {
    val b = copy(
      fields = List(SQLPart("COUNT(*) AS count", Nil)),
      group = Nil,
      having = Nil,
      sort = Nil,
      limit = None,
      offset = 0
    )
    val rs = b.execute(connection)
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  def execute(connection: Connection): ResultSet = {
    val b = new StringBuilder
    b.append("SELECT\n")
    fields.foreach { f =>
      b.append(s"\t${f.sql}\n")
    }
    b.append("FROM\n")
    b.append(s"\t${collection.name}\n")
    filters.zipWithIndex.foreach {
      case (f, index) =>
        if (index == 0) {
          b.append("WHERE\n")
        } else {
          b.append("AND\n")
        }
        b.append(s"\t${f.sql}\n")
    }
    if (group.nonEmpty) {
      b.append("GROUP BY\n\t")
      b.append(group.map(_.sql).mkString(", "))
      b.append('\n')
    }
    having.zipWithIndex.foreach {
      case (f, index) =>
        if (index == 0) {
          b.append("HAVING\n")
        } else {
          b.append("AND\n")
        }
        b.append(s"\t${f.sql}\n")
    }
    if (sort.nonEmpty) {
      b.append("ORDER BY\n\t")
      b.append(sort.map(_.sql).mkString(", "))
      b.append('\n')
    }
    limit.foreach { l =>
      b.append("LIMIT\n")
      b.append(s"\t$l\n")
    }
    if (offset > 0) {
      b.append("OFFSET\n")
      b.append(s"\t$offset\n")
    }
    val ps = connection.prepareStatement(b.toString())
    val args = (fields ::: filters ::: sort).flatMap(_.args)
    args.zipWithIndex.foreach {
      case (value, index) => SQLIndexer.setValue(ps, index + 1, value)
    }
    ps.executeQuery()
  }
}

case class SQLPart(sql: String, args: List[Json])

class SQLInserts[D <: Document[D]](ps: PreparedStatement,
                                   indexes: List[Index[_, D]],
                                   maxBatch: Int) {
  private var batchSize = 0

  def insert(doc: D): Unit = {
    indexes.map(_.getJson(doc)).zipWithIndex.foreach {
      case (value, index) => SQLIndexer.setValue(ps, index, value)
    }
    synchronized {
      batchSize += 1
      if (batchSize >= maxBatch) {
        ps.executeBatch()
        batchSize = 0
      }
    }
  }

  def execute(): Unit = synchronized {
    if (batchSize > 0) {
      ps.executeBatch()
      batchSize = 0
    }
  }

  def close(): Unit = ps.close()
}

object SQLIndexer {
  private[sql] def setValue(ps: PreparedStatement, index: Int, value: Json): Unit = value match {
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
}