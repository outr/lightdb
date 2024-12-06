package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery, AggregateType}
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.{Condition, Filter}
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import lightdb.util.ActionIterator
import lightdb._
import lightdb.field.{Field, IndexingState}
import lightdb.field.Field._

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.language.implicitConversions

abstract class SQLStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String, model: Model) extends Store[Doc, Model](name, model) {
  protected def connectionShared: Boolean
  protected def connectionManager: ConnectionManager

  transaction { implicit transaction =>
    initTransaction()
  }

  protected def createTable()(implicit transaction: Transaction[Doc]): Unit = {
    val entries = fields.collect {
      case field if !field.rw.definition.className.contains("lightdb.spatial.GeoPoint") =>
        if (field == model._id) {
          "_id VARCHAR NOT NULL PRIMARY KEY"
        } else {
          val t = def2Type(field.name, field.rw.definition)
          s"${field.name} $t"
        }
    }.mkString(", ")
    executeUpdate(s"CREATE TABLE $name($entries)")
  }

  private def def2Type(name: String, d: DefType): String = d match {
    case DefType.Str | DefType.Json | DefType.Obj(_, _) | DefType.Arr(_) | DefType.Poly(_, _) | DefType.Enum(_, _) =>
      "VARCHAR"
    case DefType.Int => "BIGINT"
    case DefType.Bool => "TINYINT"
    case DefType.Dec => "DOUBLE"
    case DefType.Opt(d) => def2Type(name, d)
    case d => throw new UnsupportedOperationException(s"$name has an unsupported type: $d")
  }

  protected def addColumn(field: Field[Doc, _])(implicit transaction: Transaction[Doc]): Unit = {
    scribe.info(s"Adding column $name.${field.name}")
    executeUpdate(s"ALTER TABLE $name ADD COLUMN ${field.name} ${def2Type(field.name, field.rw.definition)}")
  }

  protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    val connection = connectionManager.getConnection
    val existingTables = tables(connection)
    if (!existingTables.contains(name.toLowerCase)) {
      createTable()
    }

    // Add/Remove columns
    val existingColumns = columns(connection)
    val fieldNames = fields.map(_.name.toLowerCase).toSet
    // Drop columns
    existingColumns.foreach { name =>
      if (!fieldNames.contains(name.toLowerCase)) {
        scribe.info(s"Removing column ${this.name}.$name (existing: ${existingColumns.mkString(", ")}, expected: ${fieldNames.mkString(", ")}).")
        executeUpdate(s"ALTER TABLE ${this.name} DROP COLUMN $name")
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
        executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${index.name}_idx ON $name(${index.name})")
      case index: Indexed[Doc, _] =>
        executeUpdate(s"CREATE INDEX IF NOT EXISTS ${index.name}_idx ON $name(${index.name})")
      case _: Field[Doc, _] => // Nothing to do
    }
  }

  protected def tables(connection: Connection): Set[String]

  private def columns(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT * FROM $name LIMIT 1")
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
    value = SQLState(connectionManager, transaction, this, Collection.CacheQueries)
  )

  protected def field2Value(field: Field[Doc, _]): String = "?"

  protected def insertPrefix: String = "INSERT"

  protected def upsertPrefix: String = "INSERT OR REPLACE"

  protected def createInsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$insertPrefix INTO $name(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  protected def createUpsertSQL(): String = {
    val values = fields.map(field2Value)
    s"$upsertPrefix INTO $name(${fields.map(_.name).mkString(", ")}) VALUES(${values.mkString(", ")})"
  }

  private[sql] lazy val insertSQL: String = createInsertSQL()
  private[sql] lazy val upsertSQL: String = createUpsertSQL()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val state = getState
    val indexingState = new IndexingState
    state.withInsertPreparedStatement { ps =>
      fields.zipWithIndex.foreach {
        case (field, index) => SQLArg.FieldArg(doc, field, indexingState).set(ps, index + 1)
      }
      ps.addBatch()
      state.batchInsert.incrementAndGet()
      if (state.batchInsert.get() >= Collection.MaxInsertBatch) {
        ps.executeBatch()
        state.batchInsert.set(0)
      }
    }
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val state = getState
    val indexingState = new IndexingState
    state.withUpsertPreparedStatement { ps =>
      fields.zipWithIndex.foreach {
        case (field, index) => SQLArg.FieldArg(doc, field, indexingState).set(ps, index + 1)
      }
      ps.addBatch()
      state.batchUpsert.incrementAndGet()
      if (state.batchUpsert.get() >= Collection.MaxInsertBatch) {
        ps.executeBatch()
        state.batchUpsert.set(0)
      }
    }
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean = get(idField, id).nonEmpty

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    val state = getState
    val b = new SQLQueryBuilder[Doc](
      store = this,
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
    val ps = connection.prepareStatement(s"DELETE FROM $name WHERE ${field.name} = ?")
    try {
      SQLArg.FieldArg(field, value).set(ps, 1)
      ps.executeUpdate() > 0
    } finally {
      ps.close()
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    val rs = executeQuery(s"SELECT COUNT(*) FROM $name")
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
    val rs = s.executeQuery(s"SELECT * FROM $name")
    state.register(rs)
    rs2Iterator(rs, Conversion.Doc())
  }

  private def getColumnNames(rs: ResultSet): List[String] = {
    val meta = rs.getMetaData
    val count = meta.getColumnCount
    (1 to count).toList.map(index => meta.getColumnName(index))
  }

  private def getDoc(rs: ResultSet)(implicit transaction: Transaction[Doc]): Doc = model match {
    case _ if storeMode.isIndexes =>
      storeMode match {
        case StoreMode.Indexes(storage) =>
          val id = Id[Doc](rs.getString("_id"))
          storage(id)
        case _ => throw new UnsupportedOperationException("This should not be possible")
      }
    case c: SQLConversion[Doc] => c.convertFromSQL(rs)
    case c: JsonConversion[Doc] =>
      val values = fields.map { field =>
        try {
          val json = field match {
            case _: Tokenized[_] =>
              val list = Option(rs.getString(field.name)) match {
                case Some(s) => s.split(" ").toList.map(str)
                case None => Nil
              }
              arr(list: _*)
            case _ => toJson(rs.getObject(field.name), field.rw)
          }
          field.name -> json
        } catch {
          case t: Throwable =>
            val columnNames = getColumnNames(rs).mkString(", ")
            throw new RuntimeException(s"Unable to get $name.${field.name} from [$columnNames]", t)
        }
      }
      c.convertFromJson(obj(values: _*))
    case _ =>
      val map = fields.map { field =>
        field.name -> obj2Value(rs.getObject(field.name))
      }.toMap
      model.map2Doc(map)
  }

  private def rs2Iterator[V](rs: ResultSet, conversion: Conversion[Doc, V])
                            (implicit transaction: Transaction[Doc]): Iterator[V] = new Iterator[V] {
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
          MaterializedIndex[Doc, Model](json, model).asInstanceOf[V]
        case Conversion.DocAndIndexes() =>
          val json = jsonFromFields(fields.filter(_.indexed))
          val doc = getDoc(rs)
          MaterializedAndDoc[Doc, Model](json, model, doc).asInstanceOf[V]
        case Conversion.Json(fields) =>
          jsonFromFields(fields).asInstanceOf[V]
        case Conversion.Distance(field, _, _, _) =>
          val fieldName = s"${field.name}Distance"
          val distances = JsonParser(rs.getString(fieldName)).as[List[Double]].map(d => Distance(d)).toList
          val doc = getDoc(rs)
          DistanceAndDoc(doc, distances).asInstanceOf[V]
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
    case s: String => rw.definition match {
      case DefType.Str => str(s)
      case DefType.Opt(DefType.Str) => str(s)
      case DefType.Json => JsonParser(s)
      case _ => try {
        JsonParser(s)
      } catch {
        case t: Throwable => throw new RuntimeException(s"Unable to parse: [$s] as JSON for ${rw.definition}", t)
      }
    }
    case b: Boolean => bool(b)
    case i: Int => num(i)
    case l: Long => num(l)
    case f: Float => num(f.toDouble)
    case d: Double => num(d)
    case bd: BigDecimal => num(bd)
    case v => throw new RuntimeException(s"Unsupported type: $v (${v.getClass.getName})")
  }

  protected def extraFieldsForDistance(conversion: Conversion.Distance[Doc, _]): List[SQLPart] =
    throw new UnsupportedOperationException("Distance conversions not supported")

  protected def fieldPart[V](field: Field[Doc, V]): SQLPart = SQLPart(field.name)

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V] = {
    var extraFields = List.empty[SQLPart]
    val fields = conversion match {
      case Conversion.Value(field) => List(field)
      case Conversion.Doc() | Conversion.Converted(_) => this.fields
      case Conversion.Materialized(fields) => fields
      case Conversion.DocAndIndexes() => if (storeMode.isIndexes) {
        this.fields.filter(_.indexed)
      } else {
        this.fields
      }
      case Conversion.Json(fields) => fields
      case d: Conversion.Distance[Doc, _] =>
        extraFields = extraFields ::: extraFieldsForDistance(d)
        this.fields
    }
    val state = getState
    val b = SQLQueryBuilder(
      store = this,
      state = state,
      fields = fields.map(f => fieldPart(f)) ::: extraFields,
      filters = query.filter.map(filter2Part).toList,
      group = Nil,
      having = Nil,
      sort = query.sort.collect {
        case Sort.ByField(index, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart(s"${index.name} $dir")
        case Sort.ByDistance(field, _, direction) => sortByDistance(field, direction)
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
      model = model,
      offset = query.offset,
      limit = query.limit,
      total = total,
      iteratorWithScore = iteratorWithScore,
      facetResults = Map.empty,
      transaction = transaction
    )
  }

  protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
    SQLPart(s"${field.name}Distance $dir")
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
    val sort = (query.sort ::: query.query.sort).map {
      case Sort.ByField(field, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart(s"${field.name} $dir", Nil)
      case (AggregateFunction(name, _, _), direction: SortDirection) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart(s"$name $dir", Nil)
      case t => throw new UnsupportedOperationException(s"Unsupported sort: $t")
    }
    SQLQueryBuilder(
      store = this,
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
      MaterializedAggregate[Doc, Model](json, model)
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
    case f: Filter.DrillDownFacetFilter[Doc] => throw new UnsupportedOperationException(s"SQLStore does not support Facets: $f")
    case f: Filter.Equals[Doc, _] if f.field(model).isArr =>
      val values = f.getJson(model).asVector
      val parts = values.map { json =>
        val jsonString = JsonFormatter.Compact(json)
        SQLPart(s"${f.fieldName} LIKE ?", List(SQLArg.StringArg(s"%$jsonString%")))
      }
      SQLPart.merge(parts: _*)
    case f: Filter.Equals[Doc, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NULL")
    case f: Filter.Equals[Doc, _] => SQLPart(s"${f.fieldName} = ?", List(SQLArg.FieldArg(f.field(model), f.value)))
    case f: Filter.NotEquals[Doc, _] if f.field(model).isArr =>
      val values = f.getJson(model).asVector
      val parts = values.map { json =>
        val jsonString = JsonFormatter.Compact(json)
        SQLPart(s"${f.fieldName} NOT LIKE ?", List(SQLArg.StringArg(s"%$jsonString%")))
      }
      SQLPart.merge(parts: _*)
    case f: Filter.NotEquals[Doc, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NOT NULL")
    case f: Filter.NotEquals[Doc, _] => SQLPart(s"${f.fieldName} != ?", List(SQLArg.FieldArg(f.field(model), f.value)))
    case f: Filter.Regex[Doc, _] => SQLPart(s"${f.fieldName} REGEXP ?", List(SQLArg.StringArg(f.expression)))
    case f: Filter.In[Doc, _] => SQLPart(s"${f.fieldName} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList.map(v => SQLArg.FieldArg(f.field(model), v)))
    case f: Filter.RangeLong[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", List(SQLArg.LongArg(from), SQLArg.LongArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", List(SQLArg.LongArg(to)))
      case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", List(SQLArg.LongArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.RangeDouble[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", List(SQLArg.DoubleArg(from), SQLArg.DoubleArg(to)))
      case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", List(SQLArg.DoubleArg(to)))
      case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", List(SQLArg.DoubleArg(from)))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case Filter.StartsWith(fieldName, query) => SQLPart(s"$fieldName LIKE ?", List(SQLArg.StringArg(s"$query%")))
    case Filter.EndsWith(fieldName, query) => SQLPart(s"$fieldName LIKE ?", List(SQLArg.StringArg(s"%$query")))
    case Filter.Contains(fieldName, query) => SQLPart(s"$fieldName LIKE ?", List(SQLArg.StringArg(s"%$query%")))
    case Filter.Exact(fieldName, query) => SQLPart(s"$fieldName LIKE ?", List(SQLArg.StringArg(query)))
    case f: Filter.Distance[Doc] => distanceFilter(f)
    case f: Filter.Multi[Doc] =>
      val (shoulds, others) = f.filters.partition(f => f.condition == Condition.Filter || f.condition == Condition.Should)
      if (f.minShould != 1 && shoulds.nonEmpty) {
        throw new UnsupportedOperationException("Should filtering only works in SQL for exactly one condition")
      }
      val shouldParts = shoulds.map(fc => filter2Part(fc.filter)) match {
        case Nil => Nil
        case list => List(SQLPart(
          sql = list.map(_.sql).mkString("(", " OR ", ")"),
          args = list.flatMap(_.args)
        ))
      }
      val parts = others.map { fc =>
        if (fc.boost.nonEmpty) throw new UnsupportedOperationException("Boost not supported in SQL")
        fc.condition match {
          case Condition.Must => filter2Part(fc.filter)
          case Condition.MustNot =>
            val p = filter2Part(fc.filter)
            p.copy(s"NOT(${p.sql})")
          case f => throw new UnsupportedOperationException(s"$f condition not supported in SQL")
        }
      }
      SQLPart.merge(parts ::: shouldParts: _*)
  }

  private def af2Part(f: AggregateFilter[Doc]): SQLPart = f match {
    case f: AggregateFilter.Equals[Doc, _] => SQLPart(s"${f.name} = ?", List(SQLArg.FieldArg(f.field, f.value)))
    case f: AggregateFilter.NotEquals[Doc, _] => SQLPart(s"${f.name} != ?", List(SQLArg.FieldArg(f.field, f.value)))
    case f: AggregateFilter.Regex[Doc, _] => SQLPart(s"${f.name} REGEXP ?", List(SQLArg.StringArg(f.expression)))
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
    case AggregateFilter.StartsWith(name, _, query) => SQLPart(s"$name LIKE ?", List(SQLArg.StringArg(s"$query%")))
    case AggregateFilter.EndsWith(name, _, query) => SQLPart(s"$name LIKE ?", List(SQLArg.StringArg(s"%$query")))
    case AggregateFilter.Contains(name, _, query) => SQLPart(s"$name LIKE ?", List(SQLArg.StringArg(s"%$query%")))
    case AggregateFilter.Exact(name, _, query) => SQLPart(s"$name LIKE ?", List(SQLArg.StringArg(query)))
    case f: AggregateFilter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }

  protected def executeUpdate(sql: String)(implicit transaction: Transaction[Doc]): Unit = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } catch {
      case t: Throwable => throw new RuntimeException(s"Failed to execute update: $sql", t)
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
    val ps = connection.prepareStatement(s"DELETE FROM $name")
    try {
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def dispose(): Unit = if (!connectionShared) connectionManager.dispose()
}