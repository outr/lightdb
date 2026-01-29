package lightdb.sql

import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Obj, Str, arr, bool, num, obj, str}
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery, AggregateType}
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetQuery, FacetResult, FacetResultValue, FacetValue}
import lightdb.field.Field.Tokenized
import lightdb.field.Field.FacetField
import lightdb.field.{Field, FieldAndValue, IndexingState}
import lightdb.filter.{Condition, Filter}
import lightdb.filter.{FilterPlanner, QueryOptimizer}
import lightdb.*
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.sql.dsl.TxnSqlDsl
import lightdb.sql.query.{SQLPart, SQLQuery}
import lightdb.store.{Conversion, Store}
import lightdb.store.write.WriteOp
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction, RollbackSupport}
import lightdb.util.ActionIterator
import lightdb.{Query, SearchResults, Sort, SortDirection}
import rapid.Task

import java.sql.{PreparedStatement, ResultSet, SQLException, Types}
import scala.collection.mutable
import scala.util.Try

trait SQLStoreTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]]
  extends CollectionTransaction[Doc, Model]
    with PrefixScanningTransaction[Doc, Model]
    with RollbackSupport[Doc, Model] {
  override def store: SQLStore[Doc, Model]

  def state: SQLState[Doc, Model]

  def fqn: String = store.fqn

  protected case class BestMatchPlan(scoreColumn: String,
                                     scoreField: SQLPart,
                                     joinFromSuffix: List[SQLPart],
                                     matchFilter: SQLPart,
                                     sortPart: SQLPart)

  /**
   * Backends may provide a best-match (relevance) plan when Sort.BestMatch is requested.
   *
   * If defined, the plan may:
   * - join extra FROM sources (e.g., FTS tables)
   * - add a filter that constrains the match set
   * - add a computed score column and an ORDER BY clause
   */
  protected def bestMatchPlan(filter: Option[Filter[Doc]], direction: SortDirection): Option[BestMatchPlan] = None

  /**
   * Hook for backends to provide an optimized tokenized (full-text-like) equality predicate.
   * Default behavior falls back to token-by-token LIKE matching.
   */
  protected def tokenizedEqualsPart(fieldName: String, value: String): SQLPart = {
    val tokens = value.split("\\s+").toList.filter(_.nonEmpty)
    val parts = tokens.map(t => likePart(fieldName, s"%$t%"))
    if parts.isEmpty then SQLPart.Fragment("1=1") else SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
  }

  /**
   * Hook for backends to provide an optimized tokenized inequality predicate.
   * Default behavior is NOT(all tokens present).
   */
  protected def tokenizedNotEqualsPart(fieldName: String, value: String): SQLPart = {
    val tokens = value.split("\\s+").toList.filter(_.nonEmpty)
    val inner = if tokens.isEmpty then SQLPart.Fragment("1=1") else {
      val parts = tokens.map(t => likePart(fieldName, s"%$t%"))
      SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
    }
    SQLQuery(List(SQLPart.Fragment("NOT("), inner, SQLPart.Fragment(")")))
  }

  /**
   * Hook for backends to provide an indexed membership predicate for array-like fields.
   * If None, default behavior uses LIKE against the stored JSON representation.
   */
  protected def arrayContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = None

  protected def arrayNotContainsAllParts(fieldName: String, values: List[Json]): Option[SQLPart] = None

  /**
   * Transaction-aware, type-safe SQL DSL.
   *
   * This complements `sql[V](query: String)(builder: SQLQuery => SQLQuery)` for raw ad-hoc queries.
   */
  def sql: TxnSqlDsl.Builder[Doc, Model] = TxnSqlDsl(this)

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
    case Bool(b, _) => ps.setInt(index + 1, if b then 1 else 0)
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
      if rs.next() then {
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
      if state.batchInsert.get() >= Store.MaxInsertBatch then {
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
      // Flush immediately to ensure compatibility with single-connection, auto-commit drivers
      ps.executeBatch()
      state.batchUpsert.set(0)
      state.markDirty()
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

  override def _delete(id: Id[Doc]): Task[Boolean] = _deleteInternal(store.model._id, id)

  def flushOps(ops: Seq[WriteOp[Doc]]): Task[Unit] = Task {
    val inserts = mutable.ArrayBuffer.empty[Doc]
    val upserts = mutable.ArrayBuffer.empty[Doc]
    val deletes = mutable.ArrayBuffer.empty[Id[Doc]]

    ops.foreach {
      case WriteOp.Insert(doc) => inserts += doc
      case WriteOp.Upsert(doc) => upserts += doc
      case WriteOp.Delete(id) => deletes += id
    }

    if inserts.nonEmpty then {
      state.withInsertPreparedStatement { ps =>
        inserts.foreach { doc =>
          val indexingState = new IndexingState
          store.fields.zipWithIndex.foreach {
            case (field, index) => populate(ps, field.getJson(doc, indexingState), index)
          }
          ps.addBatch()
        }
        ps.executeBatch()
        state.batchInsert.set(0)
      }
    }

    if upserts.nonEmpty then {
      state.withUpsertPreparedStatement { ps =>
        upserts.foreach { doc =>
          val indexingState = new IndexingState
          store.fields.zipWithIndex.foreach {
            case (field, index) => populate(ps, field.getJson(doc, indexingState), index)
          }
          ps.addBatch()
        }
        ps.executeBatch()
        state.batchUpsert.set(0)
        state.markDirty()
      }
    }

    if deletes.nonEmpty then {
      val connection = state.connectionManager.getConnection(state)
      val sql = SQLQuery.parse(s"DELETE FROM ${store.fqn} WHERE ${store.model._id.name} = ?")
      val ps = connection.prepareStatement(sql.query)
      try {
        deletes.foreach { id =>
          sql.fillPlaceholder(store.model._id.rw.read(id)).populate(ps, this)
          ps.addBatch()
        }
        ps.executeBatch()
      } finally {
        ps.close()
      }
    }
  }

  protected def _deleteInternal[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
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
    if SQLStoreTransaction.LogQueries then scribe.info(s"Executing Query: ${sql.query} (${sql.args.mkString(", ")})")
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

  /**
   * Stream results for an arbitrary SQLQuery without forcing a Task-wrapped SearchResults.
   *
   * This is mainly used by the transaction-aware SQL DSL.
   */
  def sqlStream[V](sql: SQLQuery)(implicit rw: RW[V]): rapid.Stream[V] = {
    rapid.Stream.fromIterator[V](Task {
      val results = resultsFor(sql)
      state.register(results.rs)
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
                         scoreColumn: Option[String] = None,
                         totalQuery: Option[SQLQuery] = None,
                         facetResults: Map[FacetField[Doc], FacetResult] = Map.empty): Task[SearchResults[Doc, Model, V]] = Task {
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
      val iterator = scoreColumn match {
        case Some(col) =>
          new Iterator[(V, Double)] {
            override def hasNext: Boolean = rs.next()
            override def next(): (V, Double) = {
              val v = row2Value(rs, conversion)
              val score = try {
                rs.getDouble(col)
              } catch {
                case _: Throwable => 0.0
              }
              (v, score)
            }
          }
        case None =>
          rs2Iterator(rs, conversion).map(v => v -> 0.0)
      }
      val ps = rs.getStatement.asInstanceOf[PreparedStatement]
      ActionIterator(iterator, onClose = () => {
        Try(rs.close())
        state.returnPreparedStatement(sql.query, ps)
      })
    })
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
    var bestMatch: Option[BestMatchPlan] = None
    val fields = query.conversion match {
      case Conversion.Value(field) => List(field)
      case Conversion.Doc() => store.fields
      case Conversion.Converted(_) => store.fields
      case Conversion.Materialized(fields) => fields
      case Conversion.DocAndIndexes() => if store.storeMode.isIndexes then {
        store.fields.filter(_.indexed)
      } else {
        store.fields
      }
      case Conversion.Json(fields) => fields
      case d: Conversion.Distance[Doc, _] =>
        extraFields = extraFields ::: extraFieldsForDistance(d)
        store.fields
    }

    val bestMatchDirectionOpt: Option[SortDirection] = query.sort.collectFirst {
      case Sort.BestMatch(direction) => direction
    }
    bestMatch = bestMatchDirectionOpt.flatMap(dir => bestMatchPlan(query.filter, dir))

    val baseFieldParts: List[SQLPart] = if bestMatch.nonEmpty then {
      // When joining auxiliary tables (e.g., FTS), disambiguate base-table columns and keep stable column labels.
      fields.map(f => SQLPart.Fragment(s"${store.fqn}.${f.name} AS ${f.name}"))
    } else {
      fields.map(f => fieldPart(f))
    }

    SQLQueryBuilder(
      store = store,
      state = state,
      fields = baseFieldParts ::: extraFields ::: bestMatch.toList.map(_.scoreField),
      filters = query.filter.map(filter2Part).toList ::: bestMatch.toList.map(_.matchFilter),
      group = Nil,
      having = Nil,
      sort = query.sort.flatMap {
        case Sort.BestMatch(_) => bestMatch.toList.map(_.sortPart)
        case Sort.ByField(index, direction) =>
          val dir = if direction == SortDirection.Descending then "DESC" else "ASC"
          List(SQLPart.Fragment(s"${index.name} $dir"))
        case Sort.ByDistance(field, _, direction) => List(sortByDistance(field, direction))
        case _ => Nil
      },
      limit = query.limit.orElse(query.pageSize),
      offset = query.offset,
      fromSuffix = bestMatch.toList.flatMap(_.joinFromSuffix)
    )
  }

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task.defer {
    val b = toSQL[V](query)
    val facetResults = if query.facets.nonEmpty then {
      computeFacets(b, query.facets)
    } else {
      Task.pure(Map.empty[FacetField[Doc], FacetResult])
    }
    facetResults.flatMap { fr =>
      val scoreCol = if query.scoreDocs || query.sort.exists(_.isInstanceOf[Sort.BestMatch]) then {
        Some("__score")
      } else {
        None
      }
      execute[V](b.query, b.offset, b.limit, query.conversion, scoreCol, if query.countTotal then Some(b.totalQuery) else None, fr)
    }
  }

  override def distinct[F](query: Query[Doc, Model, _],
                           field: Field[Doc, F],
                           pageSize: Int): rapid.Stream[F] = {
    if pageSize <= 0 then {
      rapid.Stream.empty
    } else {
      // Streaming DISTINCT with paging.
      //
      // Notes:
      // - This is exact.
      // - Missing/null values are excluded (matches OpenSearch composite agg semantics).
      // - We intentionally ignore Sort / offset / limit from the incoming query; distinct has its own paging.
      rapid.Stream.force {
        val resolveExistsChild = !store.supportsNativeExistsChild
        FilterPlanner.resolve(query.filter, store.model, resolveExistsChild = resolveExistsChild).map { resolved =>
          val optimizedFilter = if query.optimize then resolved.map(QueryOptimizer.optimize) else resolved

          // Build a base query so we can reuse existing SQL filter translation.
          val baseQ: Query[Doc, Model, F] = Query(this, Conversion.Value(field))
            .copy(
              filter = optimizedFilter,
              sort = Nil,
              offset = 0,
              limit = None,
              pageSize = None,
              countTotal = false,
              scoreDocs = false,
              minDocScore = None,
              facets = Nil,
              optimize = false // already applied above
            )

          rapid.Stream(
            Task.defer {
              val lock = new AnyRef

              var offset: Int = 0
              var emittedThisPage: Int = 0
              var currentPull: rapid.Pull[F] = rapid.Pull.fromList(Nil)
              var currentPullInitialized: Boolean = false
              var done: Boolean = false

              def closeCurrentPull(): Unit = {
                try currentPull.close.handleError(_ => Task.unit).sync()
                catch { case _: Throwable => () }
              }

              def nextPageStream(): rapid.Stream[F] = {
                val qPage = baseQ.copy(offset = offset, limit = Some(pageSize))
                val b0 = toSQL(qPage)

                // Replace SELECT list with DISTINCT(field) and add NOT NULL constraint.
                val distinctField = SQLPart.Fragment(s"DISTINCT ${field.name} AS ${field.name}")
                val notNull = SQLPart.Fragment(s"${field.name} IS NOT NULL")

                val b = b0.copy(
                  fields = List(distinctField),
                  filters = b0.filters ::: List(notNull),
                  // Stable paging order.
                  sort = List(SQLPart.Fragment(s"${field.name} ASC")),
                  limit = Some(pageSize),
                  offset = offset
                )

                rapid.Stream.fromIterator(Task {
                  val results = resultsFor(b.query)
                  val rs = results.rs
                  val it = rs2Iterator(rs, Conversion.Value(field))
                  ActionIterator(it, onClose = () => {
                    Try(rs.close())
                    results.release(state)
                  })
                })
              }

              def fetchNextPull(): Unit = {
                closeCurrentPull()
                emittedThisPage = 0
                val stream = nextPageStream()
                currentPull = rapid.Stream.task(stream).sync()
                currentPullInitialized = true
              }

              val pullTask: Task[rapid.Step[F]] = Task {
                lock.synchronized {
                  @annotation.tailrec
                  def loop(): rapid.Step[F] = {
                    if done then {
                      rapid.Step.Stop
                    } else {
                      if !currentPullInitialized then {
                        fetchNextPull()
                      }

                      currentPull.pull.sync() match {
                        case e @ rapid.Step.Emit(_) =>
                          emittedThisPage += 1
                          e
                        case rapid.Step.Skip =>
                          loop()
                        case rapid.Step.Concat(inner) =>
                          currentPull = inner
                          loop()
                        case rapid.Step.Stop =>
                          // Page drained.
                          closeCurrentPull()
                          if emittedThisPage < pageSize then {
                            done = true
                            rapid.Step.Stop
                          } else {
                            offset += pageSize
                            currentPullInitialized = false
                            loop()
                          }
                      }
                    }
                  }
                  loop()
                }
              }

              val closeTask: Task[Unit] = Task {
                lock.synchronized {
                  done = true
                  closeCurrentPull()
                }
              }

              Task.pure(rapid.Pull(pullTask, closeTask))
            }
          )
        }
      }
    }
  }

  /**
   * Generic (portable) facet implementation for SQL stores.
   *
   * Notes:
   * - This computes facets by scanning matching rows and parsing the stored facet JSON.
   * - This matches Lucene semantics used by AbstractFacetSpec (including "$ROOT$" handling for hierarchical facets).
   * - Performance is acceptable for test/small datasets; for large datasets we should add backend-specific GROUP BY / JSON explode.
   */
  private def computeFacets(base: SQLQueryBuilder[Doc, Model],
                            facetQueries: List[FacetQuery[Doc]]): Task[Map[FacetField[Doc], FacetResult]] = Task {
    val facetFields: List[FacetField[Doc]] = facetQueries.map(_.field).distinct
    if facetFields.isEmpty then {
      Map.empty[FacetField[Doc], FacetResult]
    } else {
      // Build a "no paging" query that only selects the facet columns.
      val facetSelect = base.copy(
        fields = facetFields.map(ff => SQLPart.Fragment(ff.name)),
        // IMPORTANT:
        // Lucene tie-breaks facet values (when counts are equal) by taxonomy ord, which corresponds closely to
        // "first seen while indexing". For SQL we approximate this by using the database's natural scan order.
        //
        // Do NOT force an ORDER BY here (like _id ASC) because ids aren't guaranteed to reflect insertion order
        // (e.g. random UUIDs), and that breaks AbstractFacetSpec expectations.
        sort = Nil,
        limit = None,
        offset = 0
      )
      val results = resultsFor(facetSelect.query)
      val rs = results.rs
      try {
        // Per facet field, count child labels (including "$ROOT$").
        val countsByField = mutable.HashMap.empty[FacetField[Doc], mutable.HashMap[String, Int]]
        facetFields.foreach { ff =>
          countsByField.put(ff, mutable.HashMap.empty[String, Int])
        }

        def bump(ff: FacetField[Doc], key: String): Unit = {
          val map = countsByField(ff)
          map.updateWith(key) {
            case Some(v) => Some(v + 1)
            case None => Some(1)
          }
        }

        // Precompute path per field from facetQueries (last one wins, matching SearchResults map semantics).
        val queryByField: Map[FacetField[Doc], FacetQuery[Doc]] =
          facetQueries.foldLeft(Map.empty[FacetField[Doc], FacetQuery[Doc]]) {
            case (m, fq) => m.updated(fq.field, fq)
          }

        while rs.next() do {
          facetFields.foreach { ff =>
            val fq = queryByField(ff)
            val jsonStr = rs.getString(ff.name)
            val values: List[FacetValue] = Option(jsonStr) match {
              case None => Nil
              case Some(s) if s.isEmpty => Nil
              case Some(s) =>
                // stored as JSON (List[FacetValue])
                JsonParser(s).as[List[FacetValue]]
            }

            // A missing facet list is treated as "$ROOT$" for hierarchical facets at the requested path.
            if values.isEmpty then {
              if ff.hierarchical && fq.path.isEmpty then bump(ff, "$ROOT$")
            } else {
              values.foreach { fv =>
                val path = fv.path
                if ff.hierarchical then {
                  if path.startsWith(fq.path) then {
                    val child = if path.length == fq.path.length then "$ROOT$" else path(fq.path.length)
                    bump(ff, child)
                  }
                } else {
                  val child = path.headOption.getOrElse("$ROOT$")
                  bump(ff, child)
                }
              }
            }
          }
        }

        // Materialize results
        queryByField.map { case (ff, fq) =>
          val counts = countsByField(ff).toMap
          val childrenLimit = fq.childrenLimit.getOrElse(Int.MaxValue)
          val sorted = counts.iterator
            .filter(_._1 != "$ROOT$")
            .toList
            // Deterministic ordering across backends:
            // - count desc
            // - value asc
            .sortBy { case (value, count) => (-count, value) }
          val top = sorted.take(childrenLimit).map { case (value, count) => FacetResultValue(value, count) }
          val totalCount = top.map(_.count).sum
          val childCount = counts.keySet.size
          ff -> FacetResult(top, childCount = childCount, totalCount = totalCount)
        }
      } finally {
        Try(rs.close())
        results.release(state)
      }
    }
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
          if !checkedNext then {
            nextValue = rs.next()
            checkedNext = true
          }
          nextValue
        }

        override def next(): R = {
          if !checkedNext then {
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
        val json = if f.`type` == AggregateType.Concat then {
          arr(o.toString.split(";;").toList.map(s => toJson(s, f.rw)): _*)
        } else if f.`type` == AggregateType.ConcatDistinct then {
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
        val dir = if direction == SortDirection.Descending then "DESC" else "ASC"
        SQLPart.Fragment(s"${field.name} $dir")
      case (AggregateFunction(name, _, _), direction: SortDirection) =>
        val dir = if direction == SortDirection.Descending then "DESC" else "ASC"
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
    override def next(): V = row2Value(rs, conversion)
  }

  private def row2Value[V](rs: ResultSet, conversion: Conversion[Doc, V]): V = {
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

  protected[sql] def filterToSQLPart(f: Filter[Doc]): SQLPart = filter2Part(f)

  // Keep the original helper for existing SQL translation logic.
  private def filter2Part(f: Filter[Doc]): SQLPart = filter2PartOn(f, store)

  private def filter2PartOn[C <: Document[C]](f: Filter[C], targetStore: SQLStore[C, _ <: DocumentModel[C]]): SQLPart = {
    val fields = f.fields(targetStore.model)
    f match {
      case _: Filter.MatchNone[C] => SQLPart.Fragment("1=0")
      case _: Filter.ExistsChild[_] =>
        throw new UnsupportedOperationException("ExistsChild should have been resolved before SQL translation")
      case f: Filter.DrillDownFacetFilter[C] =>
        // Facets are stored as JSON (List[FacetValue]) in a single column.
        // We implement drill-down by matching the "path" JSON substring.
        // - showOnlyThisLevel: require exact path array match (no deeper segments)
        // - otherwise: prefix match
        val pathJson = JsonFormatter.Compact(f.path.json)
        if f.showOnlyThisLevel then {
          likePart(f.fieldName, s"%\"path\":$pathJson%")
        } else if f.path.isEmpty then {
          // Drill down to "any value in this dimension" - best effort
          likePart(f.fieldName, s"%\"path\":%")
        } else {
          // Prefix match: drop trailing ']' to avoid matching only exact
          val prefix = pathJson.stripSuffix("]")
          likePart(f.fieldName, s"%\"path\":$prefix%")
        }
      // Tokenized fields: equality is interpreted as matching all whitespace-separated tokens (AND semantics)
      case f: Filter.Equals[C, _] if f.value != null && f.value != None && f.field(targetStore.model).isTokenized =>
        f.getJson(targetStore.model) match {
          case Str(s, _) =>
            tokenizedEqualsPart(f.fieldName, s)
          case _ => throw new UnsupportedOperationException(s"Unsupported tokenized equality value for ${f.fieldName}")
        }
      case f: Filter.Equals[C, _] if f.field(targetStore.model).isArr =>
        val values = f.getJson(targetStore.model).asVector
        arrayContainsAllParts(f.fieldName, values.toList).getOrElse {
          val parts = values.toList.map { json =>
            val jsonString = JsonFormatter.Compact(json)
            likePart(f.fieldName, s"%$jsonString%")
          }
          SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
        }
      case f: Filter.Equals[C, _] if f.value == null | f.value == None => SQLPart(s"${f.fieldName} IS NULL")
      case f: Filter.Equals[C, _] => SQLPart(s"${f.fieldName} = ?", f.field(targetStore.model).rw.read(f.value))
      // Tokenized fields: not equals is interpreted as NOT(all tokens present)
      case f: Filter.NotEquals[C, _] if f.value != null && f.value != None && f.field(targetStore.model).isTokenized =>
        f.getJson(targetStore.model) match {
          case Str(s, _) =>
            tokenizedNotEqualsPart(f.fieldName, s)
          case _ => throw new UnsupportedOperationException(s"Unsupported tokenized inequality value for ${f.fieldName}")
        }
      case f: Filter.NotEquals[C, _] if f.field(targetStore.model).isArr =>
        val values = f.getJson(targetStore.model).asVector
        arrayNotContainsAllParts(f.fieldName, values.toList).getOrElse {
          val parts = values.toList.map { json =>
            val jsonString = JsonFormatter.Compact(json)
            notLikePart(f.fieldName, s"%$jsonString%")
          }
          SQLQuery(parts.intersperse(SQLPart.Fragment(" AND ")))
        }
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
        if targetStore eq store then {
          distanceFilter(f.asInstanceOf[Filter.Distance[Doc]])
        } else {
          throw new UnsupportedOperationException("Distance filtering not supported on related child store")
        }
      case f: Filter.Multi[C] =>
        val (shoulds, others) = f.filters.partition(f => f.condition == Condition.Filter || f.condition == Condition.Should)
        if f.minShould != 1 && shoulds.nonEmpty then {
          throw new UnsupportedOperationException("Should filtering only works in SQL for exactly one condition")
        }
        val shouldParts = shoulds.map(fc => filter2PartOn(fc.filter, targetStore)) match {
          case Nil => Nil
          case list => List(SQLQuery(SQLPart.Fragment("(") :: list.intersperse(SQLPart.Fragment(" OR ")) ::: List(SQLPart.Fragment(")"))))
        }
        val parts: List[SQLPart] = others.flatMap { fc =>
          if fc.boost.nonEmpty then throw new UnsupportedOperationException("Boost not supported in SQL")
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
        if validParts.isEmpty then {
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
    val dir = if direction == SortDirection.Descending then "DESC" else "ASC"
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
  var FetchSize: Int = 1_000
}