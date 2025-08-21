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
import lightdb.transaction.CollectionTransaction
import lightdb.util.ActionIterator
import lightdb.{Query, SearchResults, Sort, SortDirection}
import rapid.Task

import java.sql.{PreparedStatement, ResultSet, SQLException, Types}

trait SQLStoreTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends CollectionTransaction[Doc, Model] {
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
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val indexingState = new IndexingState
    state.withUpsertPreparedStatement { ps =>

      store.fields.zipWithIndex.foreach {
        case (field, index) => populate(ps, field.getJson(doc, indexingState), index)
      }
      ps.addBatch()
      state.batchUpsert.incrementAndGet()
      if (state.batchUpsert.get() >= Store.MaxInsertBatch) {
        ps.executeBatch()
        state.batchUpsert.set(0)
      }
    }
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
      ps.executeUpdate() > 0
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
      state.withPreparedStatement(sql.query) { ps =>
        try {
          sql.populate(ps, this)
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
                         totalQuery: Option[SQLQuery] = None): Task[SearchResults[Doc, Model, V]] = Task {
    val results = resultsFor(sql)

    val rs = results.rs
    state.register(rs)
    val total = totalQuery.map { sql =>
      queryTotal(sql)
    }
    val stream = rapid.Stream.fromIterator[(V, Double)](Task {
      val iterator = rs2Iterator(rs, conversion)
      val ps = rs.getStatement.asInstanceOf[PreparedStatement]
      ActionIterator(iterator.map(v => v -> 0.0), onClose = () => state.returnPreparedStatement(sql.query, ps))
    })
    SearchResults(
      model = store.model,
      offset = offset,
      limit = limit,
      total = total,
      streamWithScore = stream,
      facetResults = Map.empty,
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
      limit = query.limit.orElse(Some(query.pageSize)),
      offset = query.offset
    )
  }

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task.defer {
    val b = toSQL[V](query)
    execute[V](b.query, b.offset, b.limit, query.conversion, if (query.countTotal) Some(b.totalQuery) else None)
  }

  override def doUpdate[V](query: Query[Doc, Model, V], updates: List[FieldAndValue[Doc, _]]): Task[Int] = Task {
    val b = toSQL[V](query)
    val q = b.updateQuery(updates)
    executeUpdate(q)
  }

  override def doDelete[V](query: Query[Doc, Model, V]): Task[Int] = Task {
    val b = toSQL[V](query)
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
      limit = query.query.limit.orElse(Some(query.query.pageSize)),
      offset = query.query.offset
    )
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = Task {
    val b = aggregate2SQLQuery(query)
    queryTotal(b.totalQuery)
  }

  override def truncate: Task[Int] = Task {
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

  private def filter2Part(f: Filter[Doc]): SQLPart = {
    val fields = f.fields(store.model)
    f match {
      case f: Filter.DrillDownFacetFilter[Doc] => throw new UnsupportedOperationException(s"SQLStore does not support Facets: $f")
      case f: Filter.Equals[Doc, _] if f.field(store.model).isArr =>
        val values = f.getJson(store.model).asVector
        val parts = values.toList.map { json =>
          val jsonString = JsonFormatter.Compact(json)
          SQLPart(s"${f.fieldName} LIKE ?", s"%$jsonString%".json)
        }
        SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
      case f: Filter.Equals[Doc, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NULL")
      case f: Filter.Equals[Doc, _] => SQLPart(s"${f.fieldName} = ?", f.field(store.model).rw.read(f.value))
      case f: Filter.NotEquals[Doc, _] if f.field(store.model).isArr =>
        val values = f.getJson(store.model).asVector
        val parts = values.toList.map { json =>
          val jsonString = JsonFormatter.Compact(json)
          SQLPart(s"${f.fieldName} NOT LIKE ?", s"%$jsonString%".json)
        }
        SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
      case f: Filter.NotEquals[Doc, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NOT NULL")
      case f: Filter.NotEquals[Doc, _] => SQLPart(s"${f.fieldName} != ?", f.field(store.model).rw.read(f.value))
      case f: Filter.Regex[Doc, _] => regexpPart(f.fieldName, f.expression)
      case f: Filter.In[Doc, _] => SQLPart(s"${f.fieldName} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList.map(v => f.field(store.model).rw.read(v)): _*)
      case f: Filter.RangeLong[Doc] => (f.from, f.to) match {
        case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", from.json, to.json)
        case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", to.json)
        case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", from.json)
        case _ => throw new UnsupportedOperationException(s"Invalid: $f")
      }
      case f: Filter.RangeDouble[Doc] => (f.from, f.to) match {
        case (Some(from), Some(to)) => SQLPart(s"${f.fieldName} BETWEEN ? AND ?", from.json, to.json)
        case (None, Some(to)) => SQLPart(s"${f.fieldName} <= ?", to.json)
        case (Some(from), None) => SQLPart(s"${f.fieldName} >= ?", from.json)
        case _ => throw new UnsupportedOperationException(s"Invalid: $f")
      }
      case Filter.StartsWith(fieldName, query) if fields.head.isArr => SQLPart(s"$fieldName LIKE ?", s"%\"$query%".json)
      case Filter.StartsWith(fieldName, query) => SQLPart(s"$fieldName LIKE ?", s"$query%".json)
      case Filter.EndsWith(fieldName, query) if fields.head.isArr => SQLPart(s"$fieldName LIKE ?", s"%$query\"%".json)
      case Filter.EndsWith(fieldName, query) => SQLPart(s"$fieldName LIKE ?", s"%$query".json)
      case Filter.Contains(fieldName, query) => SQLPart(s"$fieldName LIKE ?", s"%$query%".json)
      case Filter.Exact(fieldName, query) if fields.head.isArr => SQLPart(s"$fieldName LIKE ?", s"%\"$query\"%".json)
      case Filter.Exact(fieldName, query) => SQLPart(s"$fieldName LIKE ?", query.json)
      case f: Filter.Distance[Doc] => distanceFilter(f)
      case f: Filter.Multi[Doc] =>
        val (shoulds, others) = f.filters.partition(f => f.condition == Condition.Filter || f.condition == Condition.Should)
        if (f.minShould != 1 && shoulds.nonEmpty) {
          throw new UnsupportedOperationException("Should filtering only works in SQL for exactly one condition")
        }
        val shouldParts = shoulds.map(fc => filter2Part(fc.filter)) match {
          case Nil => Nil
          case list => List(SQLQuery(SQLPart.Fragment("(") :: list.intersperse(SQLPart.Fragment(" OR ")) ::: List(SQLPart.Fragment(")"))))
        }
        val parts: List[SQLPart] = others.flatMap { fc =>
          if (fc.boost.nonEmpty) throw new UnsupportedOperationException("Boost not supported in SQL")
          fc.condition match {
            case Condition.Must => List(filter2Part(fc.filter))
            case Condition.MustNot =>
              val p = filter2Part(fc.filter)
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
}

object SQLStoreTransaction {
  var LogQueries: Boolean = false
  var FetchSize: Int = 10_000
}