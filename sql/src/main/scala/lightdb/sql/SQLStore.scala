package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.JsonParser
import fabric.rw._
import lightdb.aggregate.{AggregateFilter, AggregateQuery, AggregateType}
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.Filter
import lightdb.materialized.{MaterializedAggregate, MaterializedIndex}
import lightdb.spatial.DistanceAndDoc
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import lightdb.util.ActionIterator
import lightdb.{Field, Id, Indexed, Query, SearchResults, Sort, SortDirection, Tokenized, UniqueIndex}

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.language.implicitConversions

abstract class SQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  protected def connectionShared: Boolean
  protected def connectionManager: ConnectionManager

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
    collection.transaction { implicit transaction =>
      initTransaction()
    }
  }

  protected def createTable()(implicit transaction: Transaction[Doc]): Unit = {
    val entries = fields.collect {
      case field if !field.rw.definition.className.contains("lightdb.spatial.GeoPoint") =>
        if (field == collection.model._id) {
          "_id VARCHAR NOT NULL PRIMARY KEY"
        } else {
          val t = def2Type(field.name, field.rw.definition)
          s"${field.name} $t"
        }
    }.mkString(", ")
    executeUpdate(s"CREATE TABLE ${collection.name}($entries)")
  }

  private def def2Type(name: String, d: DefType): String = d match {
    case DefType.Str | DefType.Json | DefType.Obj(_, _) | DefType.Arr(_) | DefType.Poly(_) | DefType.Enum(_) =>
      "VARCHAR"
    case DefType.Int | DefType.Bool => "BIGINT"
    case DefType.Opt(d) => def2Type(name, d)
    case d => throw new UnsupportedOperationException(s"$name has an unsupported type: $d")
  }

  protected def addColumn(field: Field[Doc, _])(implicit transaction: Transaction[Doc]): Unit = {
    scribe.info(s"Adding column ${collection.name}.${field.name}")
    executeUpdate(s"ALTER TABLE ${collection.name} ADD COLUMN ${field.name} ${def2Type(field.name, field.rw.definition)}")
  }

  protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    val connection = connectionManager.getConnection
    val existingTables = tables(connection)
    if (!existingTables.contains(collection.name.toLowerCase)) {
      createTable()
    }

    // Add/Remove columns
    val existingColumns = columns(connection)
    val fieldNames = fields.map(_.name.toLowerCase).toSet
    // Drop columns
    existingColumns.foreach { name =>
      if (!fieldNames.contains(name.toLowerCase)) {
        scribe.info(s"Removing column ${collection.name}.$name (existing: ${existingColumns.mkString(", ")}, expected: ${fieldNames.mkString(", ")}).")
        executeUpdate(s"ALTER TABLE ${collection.name} DROP COLUMN $name")
      }
    }
    // Add columns
    fields.foreach { field =>
      val name = field.name
      if (!existingColumns.contains(name.toLowerCase)) {
        addColumn(field)
      }
    }

    // Add indexes
    fields.foreach {
      case index: UniqueIndex[Doc, _] if index.name == "_id" => // Ignore _id
      case index: UniqueIndex[Doc, _] =>
        executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
      case index: Indexed[Doc, _] =>
        executeUpdate(s"CREATE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
      case _: Field[Doc, _] => // Nothing to do
    }
  }

  protected def tables(connection: Connection): Set[String]

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

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = transaction.put(
    key = StateKey[Doc],
    value = SQLState(connectionManager, transaction, this, collection.cacheQueries)
  )

  protected def field2Value(field: Field[Doc, _]): String = "?"

  protected def insertPrefix: String = "INSERT"

  protected def upsertPrefix: String = "INSERT OR REPLACE"

  protected def createInsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$insertPrefix INTO ${collection.name}(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  protected def createUpsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$upsertPrefix INTO ${collection.name}(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  private[sql] lazy val insertSQL: String = createInsertSQL()
  private[sql] lazy val upsertSQL: String = createUpsertSQL()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val state = getState
    state.withInsertPreparedStatement { ps =>
      fields.zipWithIndex.foreach {
        case (field, index) => SQLArg.FieldArg(doc, field).set(ps, index + 1)
      }
      ps.addBatch()
      state.batchInsert.incrementAndGet()
      if (state.batchInsert.get() >= collection.maxInsertBatch) {
        ps.executeBatch()
        state.batchInsert.set(0)
      }
    }
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val state = getState
    state.withUpsertPreparedStatement { ps =>
      fields.zipWithIndex.foreach {
        case (field, index) => SQLArg.FieldArg(doc, field).set(ps, index + 1)
      }
      ps.addBatch()
      state.batchUpsert.incrementAndGet()
      if (state.batchUpsert.get() >= collection.maxInsertBatch) {
        ps.executeBatch()
        state.batchUpsert.set(0)
      }
    }
  }

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    val state = getState
    val b = new SQLQueryBuilder[Doc](
      collection = collection,
      state = state,
      fields = fields.map(f => SQLPart(f.name)),
      filters = List(filter2Part(field === value)),
      group = Nil,
      having = Nil,
      sort = Nil,
      limit = Some(1),
      offset = 0
    )
    val results = b.execute()
    val rs = results.rs
    try {
      if (rs.next()) {
        Some(getDoc(rs))
      } else {
        None
      }
    } finally {
      rs.close()
      results.release(state)
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Boolean = {
    val connection = connectionManager.getConnection
    val ps = connection.prepareStatement(s"DELETE FROM ${collection.name} WHERE ${field.name} = ?")
    try {
      SQLArg.FieldArg(field, value).set(ps, 1)
      ps.executeUpdate() > 0
    } finally {
      ps.close()
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    val rs = executeQuery(s"SELECT COUNT(*) FROM ${collection.name}")
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = {
    val state = getState
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    state.register(s)
    val rs = s.executeQuery(s"SELECT * FROM ${collection.name}")
    state.register(rs)
    rs2Iterator(rs, Conversion.Doc())
  }

  private def getColumnNames(rs: ResultSet): List[String] = {
    val meta = rs.getMetaData
    val count = meta.getColumnCount
    (1 to count).toList.map(index => meta.getColumnName(index))
  }

  private def getDoc(rs: ResultSet): Doc = collection.model match {
    case _ if storeMode == StoreMode.Indexes =>
      val id = Id[Doc](rs.getString("_id"))
      collection.t(_ => collection.model.asInstanceOf[DocumentModel[_]]._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]] -> id)
    case c: SQLConversion[Doc] => c.convertFromSQL(rs)
    case c: JsonConversion[Doc] =>
      val values = fields.map { field =>
        try {
          val json = field match {
            case _: Tokenized[_] => arr(rs.getString(field.name).split(" ").toList.map(str): _*)
            case _ => toJson(rs.getObject(field.name), field.rw)
          }
          field.name -> json
        } catch {
          case t: Throwable =>
            val columnNames = getColumnNames(rs).mkString(", ")
            throw new RuntimeException(s"Unable to get ${collection.name}.${field.name} from [$columnNames]", t)
        }
      }
      c.convertFromJson(obj(values: _*))
    case _ =>
      val map = fields.map { field =>
        field.name -> obj2Value(rs.getObject(field.name))
      }.toMap
      collection.model.map2Doc(map)
  }

  private def rs2Iterator[V](rs: ResultSet, conversion: Conversion[Doc, V]): Iterator[V] = new Iterator[V] {
    override def hasNext: Boolean = rs.next()

    override def next(): V = {
      def jsonFromFields(fields: List[Field[Doc, _]]): Json =
        obj(fields.map(f => f.name -> toJson(rs.getObject(f.name), f.rw)): _*)
      conversion match {
        case Conversion.Value(field) => toJson(rs.getObject(field.name), field.rw).as[V](field.rw)
        case Conversion.Doc() => getDoc(rs).asInstanceOf[V]
        case Conversion.Converted(c) => c(getDoc(rs))
        case Conversion.Materialized(fields) =>
          val json = jsonFromFields(fields)
          MaterializedIndex[Doc, Model](json, collection.model).asInstanceOf[V]
        case Conversion.Json(fields) =>
          jsonFromFields(fields).asInstanceOf[V]
        case Conversion.Distance(field, _, _, _) =>
          val fieldName = s"${field.name}Distance"
          val distance = rs.getDouble(fieldName)
          val doc = getDoc(rs)
          DistanceAndDoc(doc, Distance(distance)).asInstanceOf[V]
      }
    }
  }

  private def obj2Value(obj: Any): Any = obj match {
    case null => null
    case s: String => s
    case b: java.lang.Boolean => b.booleanValue()
    case i: java.lang.Integer => i.intValue()
    case l: java.lang.Long => l.longValue()
    case f: java.lang.Float => f.doubleValue()
    case d: java.lang.Double => d.doubleValue()
    case bi: java.math.BigInteger => BigDecimal(bi)
    case bd: java.math.BigDecimal => BigDecimal(bd)
    case _ => throw new RuntimeException(s"Unsupported object: $obj (${obj.getClass.getName})")
  }

  protected def toJson(value: Any, rw: RW[_]): Json = obj2Value(value) match {
    case null => Null
    case s: String if rw.definition == DefType.Str => str(s)
    case s: String if rw.definition == DefType.Json => JsonParser(s)
    case s: String => try {
      JsonParser(s)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Unable to parse: [$s] as JSON for ${rw.definition}", t)
    }
    case b: Boolean => bool(b)
    case i: Int => num(i)
    case l: Long => num(l)
    case f: Float => num(f.toDouble)
    case d: Double => num(d)
    case bd: BigDecimal => num(bd)
    case v => throw new RuntimeException(s"Unsupported type: $v (${v.getClass.getName})")
  }

  protected def fieldNamesForDistance(conversion: Conversion.Distance[Doc]): List[String] =
    throw new UnsupportedOperationException("Distance conversions not supported")

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {
    val fieldNames = conversion match {
      case Conversion.Value(field) => List(field.name)
      case Conversion.Doc() | Conversion.Converted(_) => fields.map(_.name)
      case Conversion.Materialized(fields) => fields.map(_.name)
      case Conversion.Json(fields) => fields.map(_.name)
      case d: Conversion.Distance[Doc] => fieldNamesForDistance(d)
    }
    val state = getState
    val b = SQLQueryBuilder(
      collection = collection,
      state = state,
      fields = fieldNames.map(name => SQLPart(name)),
      filters = query.filter.map(filter2Part).toList,
      group = Nil,
      having = Nil,
      sort = query.sort.collect {
        case Sort.ByField(index, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart(s"${index.name} $dir")
        case Sort.ByDistance(field, _, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart(s"${field.name}Distance $dir")
      },
      limit = query.limit,
      offset = query.offset
    )
    val results = b.execute()
    val rs = results.rs
    state.register(rs)
    val total = if (query.countTotal) {
      Some(b.queryTotal())
    } else {
      None
    }
    val iterator = rs2Iterator(rs, conversion)
    val ps = rs.getStatement.asInstanceOf[PreparedStatement]
    val iteratorWithScore = ActionIterator(iterator.map(v => v -> 0.0), onClose = () => state.returnPreparedStatement(b.sql, ps))
    SearchResults(
      offset = query.offset,
      limit = query.limit,
      total = total,
      iteratorWithScore = iteratorWithScore,
      transaction = transaction
    )
  }

  private def aggregate2SQLQuery(query: AggregateQuery[Doc, Model])
                                (implicit transaction: Transaction[Doc]): SQLQueryBuilder[Doc] = {
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
          s"$s($pre${f.field.name}$post)"
        case None => f.field.name
      }
      SQLPart(s"$fieldName AS ${f.name}", Nil)
    }
    val filters = query.query.filter.map(filter2Part).toList
    val group = query.functions.filter(_.`type` == AggregateType.Group).map(_.name).distinct.map(s => SQLPart(s, Nil))
    val having = query.filter.map(af2Part).toList
    val sort = (query.sort ::: query.query.sort).collect {
      case Sort.ByField(field, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart(s"${field.name} $dir", Nil)
    }
    SQLQueryBuilder(
      collection = collection,
      state = getState,
      fields = fields,
      filters = filters,
      group = group,
      having = having,
      sort = sort,
      limit = query.query.limit,
      offset = query.query.offset
    )
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] = {
    val b = aggregate2SQLQuery(query)
    val results = b.execute()
    val rs = results.rs
    val state = getState
    state.register(rs)
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
    createStream[MaterializedAggregate[Doc, Model]] { rs =>
      val json = obj(query.functions.map { f =>
        val o = rs.getObject(f.name)
        val json = if (f.`type` == AggregateType.Concat) {
          arr(o.toString.split(";;").toList.map(s => toJson(s, f.rw)): _*)
        } else if (f.`type` == AggregateType.ConcatDistinct) {
          arr(o.toString.split(",").toList.map(s => toJson(s, f.rw)): _*)
        } else {
          toJson(o, f.tRW)
        }
        f.name -> json
      }: _*)
      MaterializedAggregate[Doc, Model](json, collection.model)
    }
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model])
                             (implicit transaction: Transaction[Doc]): Int = {
    val b = aggregate2SQLQuery(query)
    b.queryTotal()
  }

  protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    throw new UnsupportedOperationException("Distance filtering not supported in SQL!")

  private def filter2Part(f: Filter[Doc]): SQLPart = f match {
    case f: Filter.Equals[Doc, _] => SQLPart(s"${f.field.name} = ?", List(SQLArg.FieldArg(f.field, f.value)))
    case f: Filter.In[Doc, _] => SQLPart(s"${f.field.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList.map(v => SQLArg.FieldArg(f.field, v)))
    case f: Filter.Combined[Doc] =>
      val parts = f.filters.map(f => filter2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: Filter.RangeLong[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.field.name} BETWEEN ? AND ?", List(SQLArg.LongArg(from), SQLArg.LongArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.field.name} <= ?", List(SQLArg.LongArg(to)))
      case (Some(from), None) => SQLPart(s"${f.field.name} >= ?", List(SQLArg.LongArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.RangeDouble[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.field.name} BETWEEN ? AND ?", List(SQLArg.DoubleArg(from), SQLArg.DoubleArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.field.name} <= ?", List(SQLArg.DoubleArg(to)))
      case (Some(from), None) => SQLPart(s"${f.field.name} >= ?", List(SQLArg.DoubleArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.Parsed[Doc, _] =>
      val parts = f.query.split(" ").map { q =>
        var s = q
        if (!s.startsWith("%")) {
          s = s"%$s"
        }
        if (!s.endsWith("%")) {
          s = s"$s%"
        }
        s
      }.toList
      SQLPart(parts.map(_ => s"${f.field.name} LIKE ?").mkString(" AND "), parts.map(s => SQLArg.StringArg(s)))
    case f: Filter.Distance[Doc] => distanceFilter(f)
  }

  private def af2Part(f: AggregateFilter[Doc]): SQLPart = f match {
    case f: AggregateFilter.Equals[Doc, _] => SQLPart(s"${f.name} = ?", List(SQLArg.FieldArg(f.field, f.value)))
    case f: AggregateFilter.In[Doc, _] => SQLPart(s"${f.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList.map(v => SQLArg.FieldArg(f.field, v)))
    case f: AggregateFilter.Combined[Doc] =>
      val parts = f.filters.map(f => af2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: AggregateFilter.RangeLong[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", List(SQLArg.LongArg(from), SQLArg.LongArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", List(SQLArg.LongArg(to)))
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", List(SQLArg.LongArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", List(SQLArg.DoubleArg(from), SQLArg.DoubleArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", List(SQLArg.DoubleArg(to)))
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", List(SQLArg.DoubleArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.Parsed[_, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: AggregateFilter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }

  protected def executeUpdate(sql: String)(implicit transaction: Transaction[Doc]): Unit = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } finally {
      s.close()
    }
  }

  private def executeQuery(sql: String)(implicit transaction: Transaction[Doc]): ResultSet = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    val state = getState
    state.register(s)
    s.executeQuery(sql)
  }

  protected def concatPrefix: String = "GROUP_CONCAT"

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val connection = connectionManager.getConnection
    val ps = connection.prepareStatement(s"DELETE FROM ${collection.name}")
    try {
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def dispose(): Unit = if (!connectionShared) connectionManager.dispose()
}