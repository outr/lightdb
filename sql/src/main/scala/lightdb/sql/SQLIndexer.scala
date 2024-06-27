package lightdb.sql

import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Str, bool, num, obj, str}
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Convertible
import lightdb.Id
import lightdb.aggregate.{AggregateFilter, AggregateQuery, AggregateType}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel, SetType}
import lightdb.filter.Filter
import lightdb.index.{Index, Indexer, MaterializedAggregate, MaterializedIndex}
import lightdb.query.{Query, SearchResults, Sort, SortDirection}
import lightdb.transaction.{Transaction, TransactionKey}

import java.sql.{Connection, PreparedStatement, ResultSet, Types}

trait SQLIndexer[D <: Document[D], M <: DocumentModel[D]] extends Indexer[D, M] {
  private lazy val insertsKey: TransactionKey[SQLInserts[D]] = TransactionKey("inserts")

  protected def config: SQLConfig
  protected def connectionManager: ConnectionManager[D]

  protected def createTable(): String = {
    val entries = indexes.map { index =>
      if (index.name == "_id") {
        "_id VARCHAR PRIMARY KEY"
      } else {
        val t = def2Type(index.name, index.rw.definition)
        s"${index.name} $t"
      }
    }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS ${collection.name}($entries)"
  }

  private def def2Type(name: String, d: DefType): String = d match {
    case DefType.Str => "VARCHAR"
    case DefType.Int | DefType.Bool => "INTEGER"
    case DefType.Opt(d) => def2Type(name, d)
    case d => throw new UnsupportedOperationException(s"$name has an unsupported type: $d")
  }

  override def init(collection: Collection[D, _]): Unit = {
    super.init(collection)
    collection.transaction { implicit transaction =>
      val connection = getConnection
      val statement = connection.createStatement()
      try {
        // Create the table if it doesn't exist
        statement.executeUpdate(createTable())

        // Create columns and indexes
        val existingColumns = columns(connection)
        indexes.foreach { index =>
          if (index.name != "_id") {
            if (!existingColumns.contains(index.name.toLowerCase)) {
              statement.executeUpdate(s"ALTER TABLE ${collection.name} ADD ${index.name} ${def2Type(index.name, index.rw.definition)}")
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

  private def columns(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM ${collection.name} LIMIT 1")
    try {
      val rs = ps.executeQuery()
      val meta = rs.getMetaData
      (1 to meta.getColumnCount).map { index =>
        meta.getColumnName(index).toLowerCase
      }.toSet
    } finally {
      ps.close()
    }
  }

  protected def getConnection(implicit transaction: Transaction[D]): Connection = connectionManager.getConnection

  override def transactionEnd(transaction: Transaction[D]): Unit = {
    super.transactionEnd(transaction)
    transaction.get(insertsKey).foreach { inserts =>
      inserts.close()
    }
    connectionManager.releaseConnection(transaction)
  }

  override def commit(transaction: Transaction[D]): Unit = {
    super.commit(transaction)
    transaction.get(insertsKey).foreach { inserts =>
      inserts.execute()
    }
    connectionManager.currentConnection(transaction).foreach { connection =>
      if (!config.autoCommit) connection.commit()
    }
  }

  override def postSet(doc: D, `type`: SetType, transaction: Transaction[D]): Unit = {
    super.postSet(doc, `type`, transaction)
    indexDoc(doc)(transaction)
  }

  override def postDelete(doc: D, transaction: Transaction[D]): Unit = {
    super.postDelete(doc, transaction)
    val connection = getConnection(transaction)
    val ps = connection.prepareStatement(s"DELETE FROM ${collection.name} WHERE _id = ?")
    try {
      ps.setString(1, doc._id.value)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def truncate(transaction: Transaction[D]): Unit = {
    super.truncate(transaction)
    val connection = getConnection(transaction)
    val statement = connection.createStatement()
    try {
      statement.executeUpdate(s"DELETE FROM ${collection.name}")
    } finally {
      statement.close()
    }
  }

  override protected def countInternal(implicit transaction: Transaction[D]): Int = {
    val connection = getConnection
    val statement = connection.createStatement()
    try {
      val rs = statement.executeQuery(s"SELECT COUNT(*) FROM ${collection.name}")
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

  protected def upsertPrefix: String = "INSERT OR REPLACE"

  protected def indexDoc(doc: D)(implicit transaction: Transaction[D]): Unit = {
    val connection = getConnection
    val inserts = transaction.getOrCreate(insertsKey, {
      val sql = s"$upsertPrefix INTO ${collection.name}(${indexes.map(_.name).mkString(", ")}) VALUES (${indexes.map(_ => "?").mkString(", ")})"
      val ps = connection.prepareStatement(sql)
      // TODO: Make this configurable
      val maxBatch = 10_000
      new SQLInserts[D](ps, indexes, maxBatch)
    })
    inserts.insert(doc)
  }

  protected def concatPrefix: String = "GROUP_CONCAT"

  override def aggregate(query: AggregateQuery[D, M])
                        (implicit transaction: Transaction[D]): Iterator[MaterializedAggregate[D, M]] = {
    val connection = getConnection(transaction)
    val fields = query.functions.map { f =>
      val af = f.`type` match {
        case AggregateType.Max => Some("MAX")
        case AggregateType.Min => Some("MIN")
        case AggregateType.Avg => Some("AVG")
        case AggregateType.Sum => Some("SUM")
        case AggregateType.Count | AggregateType.CountDistinct => Some("COUNT")
        case AggregateType.Concat | AggregateType.ConcatDistinct => Some(concatPrefix)
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
    val group = query.functions.filter(_.`type` == AggregateType.Group).map(_.name).distinct.map(s => SQLPart(s, Nil))
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
    def createStream[R](f: ResultSet => R): Iterator[R] = {
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
      iterator
    }
    createStream[MaterializedAggregate[D, M]] { rs =>
      val json = obj(query.functions.map { f =>
        f.name -> getJson(rs, f.name)
      }: _*)
      MaterializedAggregate[D, M](json, collection.model)
    }
  }

  override def doSearch[V](query: Query[D, M],
                           transaction: Transaction[D],
                           conversion: Conversion[V]): SearchResults[D, V] = {
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
    def createIterator[R](f: ResultSet => R): Iterator[R] = {
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
      iterator
    }
    def idIterator: Iterator[Id[D]] = createIterator(rs => Id[D](rs.getString("_id")))
    val stream: Iterator[V] = conversion match {
      case Conversion.Id => idIterator
      case Conversion.Doc => idIterator.map { id =>
        collection(id)(transaction)
      }
      case Conversion.Materialized(indexes) => createIterator[MaterializedIndex[D, M]] { rs =>
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
      scoredIterator = stream.map(v => (v, 0.0)),
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
    case bi: java.math.BigInteger => num(BigDecimal(bi))
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
      case (None, Some(to)) => SQLPart(s"${f.index.name} <= ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} >= ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.index.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.index.name} <= ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.index.name} >= ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.Parsed[_, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: Filter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }

  private def af2Part(f: AggregateFilter[_]): SQLPart = f match {
    case f: AggregateFilter.Equals[_, _] => SQLPart(s"${f.name} = ?", List(f.getJson))
    case f: AggregateFilter.In[_, _] => SQLPart(s"${f.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.getJson)
    case f: AggregateFilter.Combined[_] =>
      val parts = f.filters.map(f => af2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: AggregateFilter.RangeLong[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", List(from.json, to.json))
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", List(to.json))
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", List(from.json))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.Parsed[_, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: AggregateFilter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }
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
      case _ => ps.setString(index, JsonFormatter.Compact(value))
    }
    case _ => ps.setString(index, JsonFormatter.Compact(value))
  }
}