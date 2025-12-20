package lightdb.sql

import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Obj, Str, arr, bool, num, obj, str}
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery, AggregateType}
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field.Tokenized
import lightdb.field.{Field, FieldAndValue, IndexingState}
import lightdb.filter.{Condition, Filter}
import lightdb._
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.sql.query.{SQLPart, SQLQuery}
import lightdb.store.{Conversion, Store}
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction}
import lightdb.util.ActionIterator
import lightdb.{Query, SearchResults, Sort, SortDirection}
import rapid.Task

import java.sql.{PreparedStatement, ResultSet, SQLException, Types}
import scala.util.Try

trait SQLStoreTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends CollectionTransaction[Doc, Model] with PrefixScanningTransaction[Doc, Model] {
  override def store: SQLStore[Doc, Model]

  def state: SQLState[Doc, Model]

  def fqn: String = store.fqn

  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    val connection = state.connectionManager.getConnection(state)
    val s = connection.createStatement()
    state.register(s)
    val rs = s.executeQuery(s"SELECT * FROM ${store.fqn}")
    state.register(rs)
    rs2Iterator(rs, Conversion.Json(store.fields))
  })

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    val connection = state.connectionManager.getConnection(state)
    val ps = connection.prepareStatement(s"SELECT * FROM ${store.fqn} WHERE ${store.model._id.name} LIKE ? ORDER BY ${store.model._id.name}")
    state.register(ps)
    ps.setString(1, s"$prefix%")
    val rs = ps.executeQuery()
    state.register(rs)
    rs2Iterator(rs, Conversion.Json(store.fields))
  })

  def populate(ps: PreparedStatement, arg: Json, index: Int): Unit = arg match {
    case Null => ps.setNull(index + 1, Types.NULL)
    case o: Obj => ps.setString(index + 1, JsonFormatter.Compact(o))
    case a: Arr => ps.setString(index + 1, JsonFormatter.Compact(a))
    case Str(s, _) => ps.setString(index + 1, s)
    case Bool(b, _) => ps.setInt(index + 1, if (b) 1 else 0)
    case NumInt(l, _) => ps.setLong(index + 1, l)
    case NumDec(bd, _) => ps.setBigDecimal(index + 1, bd.bigDecimal)
  }

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    val b = new SQLQueryBuilder[Doc, Model](
      store = store,
      state = state,
      fields = store.fields.map(f => SQLPart.Fragment(f.name)),
      filters = List(filter2Part(index === value)),
      group = Nil,
      having = Nil,
      sort = Nil,
      limit = Some(1),
      offset = 0
    )
    val results = resultsFor(b.query)
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

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    val indexingState = new IndexingState
    state.withInsertPreparedStatement { ps =>
      store.fields.zipWithIndex.foreach {
        case (field, index) => populate(ps, field.getJson(doc, indexingState), index)
      }
      ps.addBatch()
      state.batchInsert.incrementAndGet()
      if (state.batchInsert.get() >= Store.MaxInsertBatch) {
        ps.executeBatch()
        state.batchInsert.set(0)
      }
    }
    // Update facet tables
    updateFacetTables(doc, indexingState)
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val indexingState = new IndexingState
    // First delete existing facet entries for this document
    deleteFacetEntries(doc._id)
    state.withUpsertPreparedStatement { ps =>
      store.fields.zipWithIndex.foreach {
        case (field, index) => populate(ps, field.getJson(doc, indexingState), index)
      }
      ps.addBatch()
      state.batchUpsert.incrementAndGet()
      // Flush immediately to ensure compatibility with single-connection, auto-commit drivers
      ps.executeBatch()
      state.batchUpsert.set(0)
      state.markDirty()
    }
    // Update facet tables
    updateFacetTables(doc, indexingState)
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)

  override protected def _count: Task[Int] = Task {
    val rs = executeQuery(s"SELECT COUNT(*) FROM ${store.fqn}")
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val connection = state.connectionManager.getConnection(state)
    val sql = SQLQuery.parse(s"DELETE FROM ${store.fqn} WHERE ${index.name} = ?")
    val ps = connection.prepareStatement(sql.query)
    try {
      sql.fillPlaceholder(index.rw.read(value)).populate(ps, this)
      val deleted = ps.executeUpdate() > 0
      // Delete facet entries if document was deleted
      if (deleted && index.name == "_id") {
        deleteFacetEntries(value.asInstanceOf[Id[Doc]])
      }
      deleted
    } finally {
      ps.close()
    }
  }

  override protected def _commit: Task[Unit] = state.commit

  override protected def _rollback: Task[Unit] = state.rollback

  override protected def _close: Task[Unit] = state.close

  def resultsFor(sql: SQLQuery): SQLResults = {
    if (SQLStoreTransaction.LogQueries) scribe.info(s"Executing Query: ${sql.query} (${sql.args.mkString(", ")})")
    try {
      state.closePendingResults()
      state.withPreparedStatement(sql.query) { ps =>
        sql.populate(ps, this)
        SQLResults(ps.executeQuery(), sql.query, ps)
      }
    } catch {
      case t: Throwable => throw new SQLException(s"Error executing query: ${sql.query} (params: ${sql.args.mkString(" | ")})", t)
    }
  }

  def executeUpdate(sql: SQLQuery): Int = {
    try {
      state.closePendingResults()
      state.withPreparedStatement(sql.query) { ps =>
        try {
          sql.populate(ps, this)
          state.markDirty()
          ps.executeUpdate()
        } finally {
          state.returnPreparedStatement(sql.query, ps)
        }
      }
    } catch {
      case t: Throwable => throw new SQLException(s"Error executing update: ${sql.query} (params: ${sql.args.mkString(" | ")})", t)
    }
  }

  def queryTotal(sql: SQLQuery): Int = {
    state.closePendingResults()
    val results = resultsFor(sql)
    val rs = results.rs
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
      results.release(state)
    }
  }

  def sql[V](query: String)
            (builder: SQLQuery => SQLQuery)
            (implicit rw: RW[V]): Task[SearchResults[Doc, Model, V]] = Task.defer {
    val sql = SQLQuery.parse(query)
    val updated = builder(sql)
    search[V](updated)
  }

  def search[V](sql: SQLQuery)(implicit rw: RW[V]): Task[SearchResults[Doc, Model, V]] = Task {
    val results = resultsFor(sql)
    state.register(results.rs)
    val stream = rapid.Stream.fromIterator[V](Task {
      val iterator = new Iterator[V] {
        private lazy val fieldNames: List[String] = {
          val metaData = results.rs.getMetaData
          val count = metaData.getColumnCount
          (1 to count).toList.map { index =>
            metaData.getColumnName(index)
          }
        }

        override def hasNext: Boolean = results.rs.next()

        override def next(): V = {
          val fieldValues = fieldNames.map { name =>
            val obj = results.rs.getObject(name)
            obj2Value(obj) match {
              case null => Null
              case s: String => str(s)
              case b: Boolean => bool(b)
              case i: Int => num(i)
              case l: Long => num(l)
              case f: Float => num(f)
              case d: Double => num(d)
              case bd: BigDecimal => num(bd)
              case v => throw new RuntimeException(s"Unsupported type: $v (${v.getClass.getName})")
            }
          }
          val json = obj(fieldNames.zip(fieldValues): _*)
          json.as[V]
        }
      }
      val ps = results.rs.getStatement.asInstanceOf[PreparedStatement]
      ActionIterator(iterator, onClose = () => state.returnPreparedStatement(sql.query, ps))
    })
    SearchResults(
      model = store.model,
      offset = 0,
      limit = None,
      total = None,
      streamWithScore = stream.map(v => v -> 0.0),
      facetResults = Map.empty,
      transaction = this
    )
  }

  private def execute[V](sql: SQLQuery,
                         offset: Int,
                         limit: Option[Int],
                         conversion: Conversion[Doc, V],
                         totalQuery: Option[SQLQuery] = None,
                         query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task {
    // For drivers like DuckDB, ensure no pending results before new statements.
    state.closePendingResults()
    // Compute total first to avoid interfering with the main result set.
    val total = totalQuery.map { tq =>
      queryTotal(tq)
    }
    // Clear again before opening the main cursor.
    state.closePendingResults()
    val results = resultsFor(sql)
    val rs = results.rs
    state.register(rs)
    val stream = rapid.Stream.fromIterator[(V, Double)](Task {
      val iterator = rs2Iterator(rs, conversion)
      val ps = rs.getStatement.asInstanceOf[PreparedStatement]
      ActionIterator(iterator.map(v => v -> 0.0), onClose = () => {
        Try(rs.close())
        state.returnPreparedStatement(sql.query, ps)
      })
    })
    
    // Compute facet results
    val facetResults = computeFacetResults(query)
    
    SearchResults(
      model = store.model,
      offset = offset,
      limit = limit,
      total = total,
      streamWithScore = stream,
      facetResults = facetResults,
      transaction = this
    )
  }

  def toSQL[V](query: Query[Doc, Model, V]): SQLQueryBuilder[Doc, Model] = {
    var extraFields = List.empty[SQLPart]
    val fields = query.conversion match {
      case Conversion.Value(field) => List(field)
      case Conversion.Doc() => store.fields
      case Conversion.Converted(_) => store.fields
      case Conversion.Materialized(fields) => fields
      case Conversion.DocAndIndexes() => if (store.storeMode.isIndexes) {
        store.fields.filter(_.indexed)
      } else {
        store.fields
      }
      case Conversion.Json(fields) => fields
      case d: Conversion.Distance[Doc, _] =>
        extraFields = extraFields ::: extraFieldsForDistance(d)
        store.fields
    }
    SQLQueryBuilder(
      store = store,
      state = state,
      fields = fields.map(f => fieldPart(f)) ::: extraFields,
      filters = query.filter.map(filter2Part).toList,
      group = Nil,
      having = Nil,
      sort = query.sort.collect {
        case Sort.ByField(index, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart.Fragment(s"${index.name} $dir")
        case Sort.ByDistance(field, _, direction) => sortByDistance(field, direction)
      },
      limit = query.limit.orElse(query.pageSize),
      offset = query.offset
    )
  }

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task.defer {
    val b = toSQL[V](query)
    execute[V](b.query, b.offset, b.limit, query.conversion, if (query.countTotal) Some(b.totalQuery) else None, query)
  }

  override def doUpdate[V](query: Query[Doc, Model, V], updates: List[FieldAndValue[Doc, _]]): Task[Int] = Task {
    val b = toSQL[V](query).copy(
      limit = query.limit     // Fix arbitrary limits for normal queries
    )
    val q = b.updateQuery(updates)
    executeUpdate(q)
  }

  override def doDelete[V](query: Query[Doc, Model, V]): Task[Int] = Task {
    val b = toSQL[V](query).copy(
      limit = query.limit     // Fix arbitrary limits for normal queries
    )
    val q = b.deleteQuery
    executeUpdate(q)
  }

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] = {
    val b = aggregate2SQLQuery(query)
    val results = resultsFor(b.query)
    val rs = results.rs
    state.register(rs)

    def createStream[R](f: ResultSet => R): rapid.Stream[R] = rapid.Stream.fromIterator(Task {
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
    })

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
      MaterializedAggregate[Doc, Model](json, store.model)
    }
  }

  private def aggregate2SQLQuery(query: AggregateQuery[Doc, Model]): SQLQueryBuilder[Doc, Model] = {
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
      SQLPart.Fragment(s"$fieldName AS ${f.name}")
    }
    val filters = query.query.filter.map(filter2Part).toList
    val group = query.functions.filter(_.`type` == AggregateType.Group).map(_.name).distinct.map(s => SQLPart.Fragment(s))
    val having = query.filter.map(af2Part).toList
    val sort = (query.sort ::: query.query.sort).map {
      case Sort.ByField(field, direction) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart.Fragment(s"${field.name} $dir")
      case (AggregateFunction(name, _, _), direction: SortDirection) =>
        val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
        SQLPart.Fragment(s"$name $dir")
      case t => throw new UnsupportedOperationException(s"Unsupported sort: $t")
    }
    SQLQueryBuilder(
      store = store,
      state = state,
      fields = fields,
      filters = filters,
      group = group,
      having = having,
      sort = sort,
      limit = query.query.limit.orElse(query.query.pageSize),
      offset = query.query.offset
    )
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = Task {
    val b = aggregate2SQLQuery(query)
    queryTotal(b.totalQuery)
  }

  override def truncate: Task[Int] = Task {
    state.closePendingResults()
    val connection = state.connectionManager.getConnection(state)
    val ps = connection.prepareStatement(s"DELETE FROM ${store.fqn}")
    try {
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  protected def regexpPart(name: String, expression: String): SQLPart = SQLQuery(List(
    SQLPart.Fragment(s"$name REGEXP "), SQLPart.Arg(expression.json)
  ))

  protected def likePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name LIKE ?", pattern.json)

  protected def notLikePart(name: String, pattern: String): SQLPart =
    SQLPart(s"$name NOT LIKE ?", pattern.json)

  private def af2Part(f: AggregateFilter[Doc]): SQLPart = f match {
    case f: AggregateFilter.Equals[Doc, _] => SQLQuery(List(
      SQLPart.Fragment(s"${f.name} = "), SQLPart.Arg(f.field.rw.read(f.value))
    ))
    case f: AggregateFilter.NotEquals[Doc, _] =>
      SQLQuery(List(
        SQLPart.Fragment(s"${f.name} != "), SQLPart.Arg(f.field.rw.read(f.value))
      ))
    case f: AggregateFilter.Regex[Doc, _] => regexpPart(f.name, f.expression)
    case f: AggregateFilter.In[Doc, _] =>
      SQLQuery(
        SQLPart.Fragment(s"${f.name} IN (") ::
          f.values.toList.map[SQLPart](v => SQLPart.Arg(f.field.rw.read(v))).intersperse(SQLPart.Fragment(", ")) :::
          List(SQLPart.Fragment(")"))
      )
    case f: AggregateFilter.Combined[Doc] =>
      val parts = f.filters.map(f => af2Part(f))
      SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
    case f: AggregateFilter.RangeLong[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", from.json, to.json)
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", to.json)
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", from.json)
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: AggregateFilter.RangeDouble[_] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.name} BETWEEN ? AND ?", from.json, to.json)
      case (None, Some(to)) => SQLPart(s"${f.name} <= ?", to.json)
      case (Some(from), None) => SQLPart(s"${f.name} >= ?", from.json)
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case AggregateFilter.StartsWith(name, _, query) => SQLPart(s"$name LIKE ?", s"$query%".json)
    case AggregateFilter.EndsWith(name, _, query) => SQLPart(s"$name LIKE ?", s"%$query".json)
    case AggregateFilter.Contains(name, _, query) => SQLPart(s"$name LIKE ?", s"%$query%".json)
    case AggregateFilter.Exact(name, _, query) => SQLPart(s"$name LIKE ?", query.json)
    case f: AggregateFilter.Distance[_] => throw new UnsupportedOperationException("Distance not supported in SQL!")
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
          MaterializedIndex[Doc, Model](json, store.model).asInstanceOf[V]
        case Conversion.DocAndIndexes() =>
          val json = jsonFromFields(store.fields.filter(_.indexed))
          val doc = getDoc(rs)
          MaterializedAndDoc[Doc, Model](json, store.model, doc).asInstanceOf[V]
        case Conversion.Json(fields) =>
          jsonFromFields(fields).asInstanceOf[V]
        case Conversion.Distance(field, _, _, _) =>
          val fieldName = s"${field.name}Distance"
          val distances = JsonParser(rs.getString(fieldName)).as[List[Double]].map(d => Distance(d))
          val doc = getDoc(rs)
          DistanceAndDoc(doc, distances).asInstanceOf[V]
      }
    }
  }

  protected def toJson(value: Any, rw: RW[_]): Json = obj2Value(value) match {
    case null => Null
    case s: String => rw.definition match {
      case DefType.Str => str(s)
      case DefType.Opt(DefType.Str) => str(s)
      case DefType.Opt(DefType.Enum(_, _)) => str(s)
      case DefType.Json => JsonParser(s)
      case DefType.Enum(_, _) => str(s)
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
    case b: Byte => num(b)
    case v => throw new RuntimeException(s"Unsupported type: $v (${v.getClass.getName})")
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
    case b: java.lang.Byte => b.byteValue()
    case _ => throw new RuntimeException(s"Unsupported object: $obj (${obj.getClass.getName})")
  }

  private def getDoc(rs: ResultSet): Doc = store.model match {
    case _ if store.storeMode.isIndexes =>
      val id = Id[Doc](rs.getString("_id"))
      parent.get(id).sync()
    case c: SQLConversion[Doc] => c.convertFromSQL(rs)
    case c: JsonConversion[Doc] =>
      val values = store.fields.map { field =>
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
            throw new RuntimeException(s"Unable to get ${store.fqn}.${field.name} from [$columnNames]", t)
        }
      }
      c.convertFromJson(obj(values: _*))
    case _ =>
      val map = store.fields.map { field =>
        field.name -> obj2Value(rs.getObject(field.name))
      }.toMap
      store.model.map2Doc(map)
  }

  private def getColumnNames(rs: ResultSet): List[String] = {
    val meta = rs.getMetaData
    val count = meta.getColumnCount
    (1 to count).toList.map(index => meta.getColumnName(index))
  }

  private def filter2Part(f: Filter[Doc]): SQLPart = filter2PartOn(f, store)

  private def filter2PartOn[C <: Document[C]](f: Filter[C], targetStore: SQLStore[C, _ <: DocumentModel[C]]): SQLPart = {
    val fields = f.fields(targetStore.model)
    f match {
      case _: Filter.MatchNone[C] => SQLPart.Fragment("1=0")
      case _: Filter.ExistsChild[_, _, _] =>
        throw new UnsupportedOperationException("ExistsChild should have been resolved before SQL translation")
      case f: Filter.DrillDownFacetFilter[C] => 
        // Get the facet field from the model
        val modelField = targetStore.model.fieldByName(f.fieldName)
        val facetField = modelField match {
          case ff: Field.FacetField[C @unchecked] => ff
          case _ => throw new RuntimeException(s"Field ${f.fieldName} is not a FacetField")
        }
        val tableName = targetStore.facetTableName(facetField)
        
        if (f.showOnlyThisLevel) {
          // Only match documents where the facet path ends exactly at this level (has $ROOT$ marker)
          val fullPath = if (f.path.isEmpty) "$ROOT$" else f.path.mkString("/") + "/$ROOT$"
          SQLPart(
            s"${targetStore.model._id.name} IN (SELECT doc_id FROM $tableName WHERE full_path = ?)",
            str(fullPath)
          )
        } else {
          // Match documents that have this prefix in their facet path
          val pathFilter = if (f.path.isEmpty) {
            // No path means match all documents with this facet
            s"${targetStore.model._id.name} IN (SELECT DISTINCT doc_id FROM $tableName)"
          } else {
            val fullPath = f.path.mkString("/")
            s"${targetStore.model._id.name} IN (SELECT DISTINCT doc_id FROM $tableName WHERE full_path = ? OR full_path LIKE ?)"
          }
          
          if (f.path.isEmpty) {
            SQLPart.Fragment(pathFilter)
          } else {
            val fullPath = f.path.mkString("/")
            SQLPart(pathFilter, str(fullPath), str(s"$fullPath/%"))
          }
        }
      // Tokenized fields: equality is interpreted as matching all whitespace-separated tokens (AND semantics)
      case f: Filter.Equals[C, _] if f.value != null && f.value != None && f.field(targetStore.model).isTokenized =>
        f.getJson(targetStore.model) match {
          case Str(s, _) =>
            val tokens = s.split("\\s+").toList.filter(_.nonEmpty)
            val parts = tokens.map(t => likePart(f.fieldName, s"%$t%"))
            if (parts.isEmpty) SQLPart.Fragment("1=1") else SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
          case _ => throw new UnsupportedOperationException(s"Unsupported tokenized equality value for ${f.fieldName}")
        }
      case f: Filter.Equals[C, _] if f.field(targetStore.model).isArr =>
        val values = f.getJson(targetStore.model).asVector
        val parts = values.toList.map { json =>
          val jsonString = JsonFormatter.Compact(json)
          likePart(f.fieldName, s"%$jsonString%")
        }
        SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
      case f: Filter.Equals[C, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NULL")
      case f: Filter.Equals[C, _] => SQLPart(s"${f.fieldName} = ?", f.field(targetStore.model).rw.read(f.value))
      // Tokenized fields: not equals is interpreted as NOT(all tokens present)
      case f: Filter.NotEquals[C, _] if f.value != null && f.value != None && f.field(targetStore.model).isTokenized =>
        f.getJson(targetStore.model) match {
          case Str(s, _) =>
            val tokens = s.split("\\s+").toList.filter(_.nonEmpty)
            val inner = if (tokens.isEmpty) SQLPart.Fragment("1=1") else {
              val parts = tokens.map(t => likePart(f.fieldName, s"%$t%"))
              SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
            }
            SQLQuery(List(SQLPart.Fragment("NOT("), inner, SQLPart.Fragment(")")))
          case _ => throw new UnsupportedOperationException(s"Unsupported tokenized inequality value for ${f.fieldName}")
        }
      case f: Filter.NotEquals[C, _] if f.field(targetStore.model).isArr =>
        val values = f.getJson(targetStore.model).asVector
        val parts = values.toList.map { json =>
          val jsonString = JsonFormatter.Compact(json)
          notLikePart(f.fieldName, s"%$jsonString%")
        }
        SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
      case f: Filter.NotEquals[C, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NOT NULL")
      case f: Filter.NotEquals[C, _] => SQLPart(s"${f.fieldName} != ?", f.field(targetStore.model).rw.read(f.value))
      case f: Filter.Regex[C, _] => regexpPart(f.fieldName, f.expression)
      // TODO: Support fieldName = ANY (?::text[])
      case f: Filter.In[C, _] => SQLPart(s"${f.fieldName} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList.map(v => f.field(targetStore.model).rw.read(v)): _*)
      case f: Filter.RangeLong[C] => (f.from, f.to) match {
        case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", from.json, to.json)
        case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", to.json)
        case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", from.json)
        case _ => throw new UnsupportedOperationException(s"Invalid: $f")
      }
      case f: Filter.RangeDouble[C] => (f.from, f.to) match {
        case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", from.json, to.json)
        case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", to.json)
        case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", from.json)
        case _ => throw new UnsupportedOperationException(s"Invalid: $f")
      }
      case Filter.StartsWith(fieldName, query) if fields.head.isArr => likePart(fieldName, s"%\"$query%")
      case Filter.StartsWith(fieldName, query) => likePart(fieldName, s"$query%")
      case Filter.EndsWith(fieldName, query) if fields.head.isArr => likePart(fieldName, s"%$query\"%")
      case Filter.EndsWith(fieldName, query) => likePart(fieldName, s"%$query")
      case Filter.Contains(fieldName, query) => likePart(fieldName, s"%$query%")
      case Filter.Exact(fieldName, query) if fields.head.isArr => likePart(fieldName, s"%\"$query\"%")
      case Filter.Exact(fieldName, query) => likePart(fieldName, s"$query")
      case f: Filter.Distance[C] =>
        if (targetStore eq store) {
          distanceFilter(f.asInstanceOf[Filter.Distance[Doc]])
        } else {
          throw new UnsupportedOperationException("Distance filtering not supported on related child store")
        }
      case f: Filter.Multi[C] =>
        val (shoulds, others) = f.filters.partition(f => f.condition == Condition.Filter || f.condition == Condition.Should)
        if (f.minShould != 1 && shoulds.nonEmpty) {
          throw new UnsupportedOperationException("Should filtering only works in SQL for exactly one condition")
        }
        val shouldParts = shoulds.map(fc => filter2PartOn(fc.filter, targetStore)) match {
          case Nil => Nil
          case list => List(SQLQuery(SQLPart.Fragment("(") :: list.intersperse(SQLPart.Fragment(" OR ")) ::: List(SQLPart.Fragment(")"))))
        }
        val parts: List[SQLPart] = others.flatMap { fc =>
          if (fc.boost.nonEmpty) throw new UnsupportedOperationException("Boost not supported in SQL")
          fc.condition match {
            case Condition.Must => List(filter2PartOn(fc.filter, targetStore))
            case Condition.MustNot =>
              val p = filter2PartOn(fc.filter, targetStore)
              // Create a proper SQLQuery for NOT conditions to avoid flattening issues
              List(SQLQuery(List(SQLPart.Fragment("NOT("), p, SQLPart.Fragment(")"))))
            case f => throw new UnsupportedOperationException(s"$f condition not supported in SQL")
          }
        }
        // Filter out any empty or invalid parts before creating the SQL
        val validParts = (parts ::: shouldParts).filter {
          case SQLPart.Fragment("") => false
          case _ => true
        }
        if (validParts.isEmpty) {
          SQLPart.Fragment("1=1") // Always true condition as fallback
        } else {
          SQLQuery(validParts.intersperse(SQLPart.Fragment(" AND ")))
        }
    }
  }

  private def executeQuery(sql: String): ResultSet = {
    val connection = state.connectionManager.getConnection(state)
    val s = connection.createStatement()
    state.register(s)
    s.executeQuery(sql)
  }

  protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = {
    val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
    SQLPart.Fragment(s"${field.name}Distance $dir")
  }

  protected def fieldPart[V](field: Field[Doc, V]): SQLPart = SQLPart.Fragment(field.name)

  protected def concatPrefix: String = "GROUP_CONCAT"

  protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    throw new UnsupportedOperationException("Distance filtering not supported in SQL!")

  protected def extraFieldsForDistance(conversion: Conversion.Distance[Doc, _]): List[SQLPart] =
    throw new UnsupportedOperationException("Distance conversions not supported")

  /**
   * Update facet tables for a document.
   */
  private def updateFacetTables(doc: Doc, indexingState: IndexingState): Unit = {
    import lightdb.field.Field.FacetField
    import lightdb.facet.FacetValue
    
    val docId = doc._id.toString
    
    store.facetFields.foreach { facetField =>
      val tableName = store.facetTableName(facetField)
      val facetValues = facetField.get(doc, facetField, indexingState)
      
      facetValues.foreach { facetValue =>
        insertFacetEntries(tableName, docId, facetValue, facetField.hierarchical)
      }
    }
  }

  /**
   * Insert facet entries for a facet value.
   */
  private def insertFacetEntries(tableName: String, docId: String, facetValue: FacetValue, hierarchical: Boolean): Unit = {
    val connection = state.connectionManager.getConnection(state)
    val path = facetValue.path
    
    if (hierarchical) {
      // For hierarchical facets, insert entries for each level of the hierarchy
      // e.g., for path ["2010", "10", "15"], insert:
      //   - depth=0, component="2010", full_path="2010"
      //   - depth=1, component="10", full_path="2010/10"
      //   - depth=2, component="15", full_path="2010/10/15"
      //   - depth=3, component="$ROOT$", full_path="2010/10/15/$ROOT$" (marks end)
      val fullPath = path.mkString("/")
      var currentPath = ""
      
      path.zipWithIndex.foreach { case (component, depth) =>
        currentPath = if (currentPath.isEmpty) component else s"$currentPath/$component"
        val insertSQL = s"INSERT INTO $tableName (doc_id, path_depth, path_component, full_path) VALUES (?, ?, ?, ?)"
        val ps = connection.prepareStatement(insertSQL)
        try {
          ps.setString(1, docId)
          ps.setInt(2, depth)
          ps.setString(3, component)
          ps.setString(4, currentPath)
          ps.executeUpdate()
          state.markDirty()
        } finally {
          ps.close()
        }
      }
      
      // Add the $ROOT$ marker for hierarchical facets
      val rootInsertSQL = s"INSERT INTO $tableName (doc_id, path_depth, path_component, full_path) VALUES (?, ?, ?, ?)"
      val rootPs = connection.prepareStatement(rootInsertSQL)
      try {
        rootPs.setString(1, docId)
        rootPs.setInt(2, path.length)
        rootPs.setString(3, "$ROOT$")
        rootPs.setString(4, if (fullPath.isEmpty) "$ROOT$" else s"$fullPath/$$ROOT$$")
        rootPs.executeUpdate()
        state.markDirty()
      } finally {
        rootPs.close()
      }
    } else {
      // For non-hierarchical facets, insert a single entry
      val component = if (path.isEmpty) "" else path.mkString("/")
      val insertSQL = s"INSERT INTO $tableName (doc_id, path_depth, path_component, full_path) VALUES (?, ?, ?, ?)"
      val ps = connection.prepareStatement(insertSQL)
      try {
        ps.setString(1, docId)
        ps.setInt(2, 0)
        ps.setString(3, component)
        ps.setString(4, component)
        ps.executeUpdate()
        state.markDirty()
      } finally {
        ps.close()
      }
    }
  }

  /**
   * Delete all facet entries for a document.
   */
  private def deleteFacetEntries(docId: Id[Doc]): Unit = {
    val connection = state.connectionManager.getConnection(state)
    store.facetFields.foreach { facetField =>
      val tableName = store.facetTableName(facetField)
      val deleteSQL = s"DELETE FROM $tableName WHERE doc_id = ?"
      val ps = connection.prepareStatement(deleteSQL)
      try {
        ps.setString(1, docId.toString)
        ps.executeUpdate()
        state.markDirty()
      } finally {
        ps.close()
      }
    }
  }

  /**
   * Compute facet results for a query.
   */
  private def computeFacetResults[V](query: Query[Doc, Model, V]): Map[Field.FacetField[Doc], lightdb.facet.FacetResult] = {
    import lightdb.facet.{FacetResult, FacetResultValue}
    
    if (query.facets.isEmpty) {
      Map.empty
    } else {
      query.facets.map { fq =>
        val facetField = fq.field
        val tableName = store.facetTableName(facetField)
        val parentPath = fq.path
        val childrenLimit = fq.childrenLimit
        
        // Build the query to get facet counts
        // We need to:
        // 1. Filter by the parent path (if any)
        // 2. Count documents by the next level component
        // 3. Apply the main query filter to only count matching documents
        
        val pathDepth = parentPath.length
        val pathFilter = if (parentPath.isEmpty) {
          // Root level: get all depth=0 components
          s"path_depth = $pathDepth"
        } else {
          // Get children of parent path: depth = parentPath.length, and full_path starts with parent
          val parentFullPath = parentPath.mkString("/")
          s"path_depth = $pathDepth AND full_path LIKE '$parentFullPath/%'"
        }
        
        // Build a filter to only include documents that match the main query
        val docIdFilter = query.filter match {
          case Some(filter) =>
            // Build the main query SQL to get matching doc IDs
            val b = toSQL[Doc](query.copy(
              facets = Nil,
              conversion = Conversion.Doc(),
              limit = None,
              offset = 0
            ))
            // Get just the filter parts
            val filterParts = b.filters
            if (filterParts.nonEmpty) {
              val conditions = filterParts.map(_.query).mkString(" AND ")
              s"doc_id IN (SELECT ${store.model._id.name} FROM ${store.fqn} WHERE $conditions)"
            } else {
              "1=1"
            }
          case None =>
            "1=1" // No filter, include all
        }
        
        // Query to get facet counts
        val facetQuery = s"""
          SELECT 
            path_component,
            COUNT(DISTINCT doc_id) as count
          FROM $tableName
          WHERE $pathFilter
            AND path_component != '$$ROOT$$'
            AND $docIdFilter
          GROUP BY path_component
          ORDER BY count DESC, path_component ASC
          ${childrenLimit.map(l => s"LIMIT $l").getOrElse("")}
        """.trim
        
        val results = try {
          val rs = executeQuery(facetQuery)
          try {
            var values = List.empty[FacetResultValue]
            var totalCount = 0
            while (rs.next()) {
              val value = rs.getString("path_component")
              val count = rs.getInt("count")
              values = FacetResultValue(value, count) :: values
              totalCount += count
            }
            FacetResult(values.reverse, values.length, totalCount)
          } finally {
            rs.close()
          }
        } catch {
          case t: Throwable =>
            scribe.warn(s"Failed to compute facet for ${facetField.name}: ${t.getMessage}", t)
            FacetResult(Nil, 0, 0)
        }
        
        facetField -> results
      }.toMap
    }
  }
}

object SQLStoreTransaction {
  var LogQueries: Boolean = false
  var FetchSize: Int = 1_000
}