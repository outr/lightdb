package lightdb.arangodb

import com.arangodb.ArangoDBException
import com.arangodb.entity.ErrorEntity
import com.arangodb.model.{DocumentCreateOptions, OverwriteMode}
import com.arangodb.util.RawJson
import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.SortDirection
import lightdb.aggregate.{AggregateFunction, AggregateQuery, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DuplicateIdException
import lightdb.facet.FacetComputation
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{FilterPlanner, NestedQuerySupport, QueryOptimizer}
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.store.Conversion
import lightdb.store.write.WriteOp
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction}
import lightdb.{Query, SearchResults}
import rapid.Task

import scala.jdk.CollectionConverters.*

case class ArangoDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: ArangoDBStore[Doc, Model],
  parent: Option[lightdb.transaction.Transaction[Doc, Model]],
  writeHandlerFactory: lightdb.transaction.Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends CollectionTransaction[Doc, Model]
  with PrefixScanningTransaction[Doc, Model]
  with lightdb.graph.NativeGraphTraversal {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private val DuplicateKeyErrorNum = 1210

  private def collection = store.collection

  // -- Json <-> ArangoDB document ------------------------------------------------------------------
  // Stored body = the document's fields merged with every indexed-field projection (so computed
  // indexed fields and tokenized fields are queryable via AQL). ArangoDB-reserved attribute names
  // (`_id`->`_key`, `_from`/`_to`, which ArangoDB strips in document collections) are escaped via
  // `ArangoQuery.escapeKey`; tokenized fields are stored as token arrays. On read, ArangoDB's system
  // `_id`/`_rev` are dropped and the escaped names are restored.
  private def toBody(doc: Doc): String = {
    val state = new IndexingState
    val base: Map[String, Json] = doc.json(store.model.rw) match {
      case o: Obj => o.value
      case _ => Map.empty[String, Json]
    }
    val merged = store.fields.foldLeft(base) { (m, f) =>
      val raw = f.getJson(doc, state)
      val stored = if f.isTokenized then raw match {
        case Str(s, _) => arr(ArangoQuery.tokenize(s).map(str)*)
        case other => other
      } else raw
      m.updated(f.name, stored)
    }
    val escaped: Map[String, Json] = merged.map { case (k, v) => ArangoQuery.escapeKey(k) -> v }
    // For edge collections, set ArangoDB's system `_from`/`_to` to document handles in the shared
    // vertex namespace so AQL OUTBOUND traversal can follow them. The LightDB values remain under the
    // escaped `from_`/`to_` and are authoritative on read.
    val finalMap = if store.isEdgeModel then {
      def handle(v: Option[Json]): Json = v match {
        case Some(Str(s, _)) => str(s"${store.vertexNamespace}/$s")
        case _ => Null
      }
      escaped + ("_from" -> handle(base.get("_from"))) + ("_to" -> handle(base.get("_to")))
    } else escaped
    JsonFormatter.Compact(Obj(finalMap))
  }

  // Drop ArangoDB system attributes (`_id`=coll/key, `_rev`, edge `_from`/`_to` handles); restore
  // escaped names (e.g. `_key`->`_id`, `from_`->`_from`).
  private def mapArangoDoc(json: Json): Json = json match {
    case o: Obj => Obj(o.value.collect {
      case (k, v) if k != "_id" && k != "_rev" && k != "_from" && k != "_to" => ArangoQuery.unescapeKey(k) -> v
    })
    case other => other
  }

  private def toJson(raw: RawJson): Json = mapArangoDoc(JsonParser(raw.get()))

  private def fromRaw(raw: RawJson): Doc = toJson(raw).as[Doc](store.model.rw)

  private def runQuery(aql: String): List[RawJson] =
    store.database.query(aql, classOf[RawJson], Map[String, Any]("@col" -> store.arangoName).asJava).asListRemaining.asScala.toList

  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    val cursor = store.database.query("FOR d IN @@col RETURN d", classOf[RawJson], Map[String, Any]("@col" -> store.arangoName).asJava)
    new Iterator[Json] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): Json = toJson(cursor.next())
    }
  })

  // Prefix-scan over LightDB `_id` (stored as `_key`); backs the traversal module's edge lookups.
  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    val cursor = store.database.query(
      "FOR d IN @@col FILTER STARTS_WITH(d.`_key`, @prefix) SORT d.`_key` RETURN d",
      classOf[RawJson],
      Map[String, Any]("@col" -> store.arangoName, "prefix" -> prefix).asJava
    )
    new Iterator[Json] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): Json = toJson(cursor.next())
    }
  })

  // -- Native graph traversal (AQL OUTBOUND) -------------------------------------------------------

  override def nativeTraverseEdges(startKeys: List[String], maxDepth: Int, breadthFirst: Boolean): rapid.Stream[Json] =
    if startKeys.isEmpty then rapid.Stream.empty
    else rapid.Stream.fromIterator(Task {
      val starts = startKeys.map(k => s"${store.vertexNamespace}/$k")
      // `uniqueVertices: 'global'` requires BFS; DFS uses path-uniqueness.
      val options = if breadthFirst then "{bfs: true, uniqueVertices: 'global'}" else "{bfs: false, uniqueVertices: 'path'}"
      val depth = if maxDepth == Int.MaxValue then Int.MaxValue.toLong else maxDepth.toLong
      val aql = s"FOR s IN @starts FOR v, e IN 1..$depth OUTBOUND s @@col OPTIONS $options RETURN e"
      val cursor = store.database.query(aql, classOf[RawJson], Map[String, Any]("@col" -> store.arangoName, "starts" -> starts.asJava).asJava)
      new Iterator[Json] {
        override def hasNext: Boolean = cursor.hasNext
        override def next(): Json = toJson(cursor.next())
      }
    })

  override def nativeShortestPathEdges(fromKey: String, toKey: String): rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    val from = s"${store.vertexNamespace}/$fromKey"
    val to = s"${store.vertexNamespace}/$toKey"
    // SHORTEST_PATH yields (vertex, edge) pairs; the start vertex's edge is null — filter it out.
    val aql = "FOR v, e IN OUTBOUND SHORTEST_PATH @from TO @to @@col FILTER e != null RETURN e"
    val cursor = store.database.query(aql, classOf[RawJson], Map[String, Any]("@col" -> store.arangoName, "from" -> from, "to" -> to).asJava)
    new Iterator[Json] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): Json = toJson(cursor.next())
    }
  })

  override def nativeAllPaths(fromKey: String, toKey: String, maxDepth: Int): rapid.Stream[List[Json]] = rapid.Stream.fromIterator(Task {
    val from = s"${store.vertexNamespace}/$fromKey"
    val to = s"${store.vertexNamespace}/$toKey"
    val max = if maxDepth == Int.MaxValue then 100 else maxDepth // K_PATHS requires a finite bound
    val aql = s"FOR p IN 1..$max OUTBOUND K_PATHS @from TO @to @@col RETURN p.edges"
    val cursor = store.database.query(aql, classOf[RawJson], Map[String, Any]("@col" -> store.arangoName, "from" -> from, "to" -> to).asJava)
    new Iterator[List[Json]] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): List[Json] = JsonParser(cursor.next().get()) match {
        case Arr(values, _) => values.map(mapArangoDoc).toList
        case _ => Nil
      }
    }
  })

  // -- Raw AQL ------------------------------------------------------------------------------------
  // Parity with SQL's raw-query support. `@@col` is auto-bound to this store's collection when the
  // query references it; the caller supplies any other bind parameters.

  /** Execute a raw AQL query, returning each result row as JSON exactly as ArangoDB returns it. */
  def aql(query: String, bindVars: Map[String, Any] = Map.empty): rapid.Stream[Json] =
    rawAql(query, bindVars).map(r => JsonParser(r.get()))

  /** Execute a raw AQL query that returns documents of this store, mapped back to `Doc`. */
  def aqlDocs(query: String, bindVars: Map[String, Any] = Map.empty): rapid.Stream[Doc] =
    rawAql(query, bindVars).map(fromRaw)

  private def rawAql(query: String, bindVars: Map[String, Any]): rapid.Stream[RawJson] = rapid.Stream.fromIterator(Task {
    val binds = if query.contains("@@col") then bindVars + ("@col" -> store.arangoName) else bindVars
    val cursor = store.database.query(query, classOf[RawJson], binds.asJava)
    new Iterator[RawJson] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): RawJson = cursor.next()
    }
  })

  // -- KV contract -----------------------------------------------------------------------------------

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if index == store.idField then {
      Option(collection.getDocument(value.asInstanceOf[Id[Doc]].value, classOf[RawJson])).map(fromRaw)
    } else {
      throw new UnsupportedOperationException(s"ArangoDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    collection.insertDocument(RawJson.of(toBody(doc)), new DocumentCreateOptions().overwriteMode(OverwriteMode.replace))
    doc
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    try {
      collection.insertDocument(RawJson.of(toBody(doc)))
      doc
    } catch {
      case e: ArangoDBException if e.getErrorNum == DuplicateKeyErrorNum =>
        throw DuplicateIdException(store.name, doc._id)
    }
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task(collection.documentExists(id.value))

  override protected def _count: Task[Int] = Task(collection.count().getCount.toInt)

  override protected def _delete(id: Id[Doc]): Task[Boolean] = Task {
    try {
      collection.deleteDocument(id.value)
      true
    } catch {
      case e: ArangoDBException if e.getResponseCode != null && e.getResponseCode == 404 => false
    }
  }

  override protected def _commit: Task[Unit] = Task.unit
  override protected def _rollback: Task[Unit] = Task.unit
  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task {
    val count = collection.count().getCount.toInt
    collection.truncate()
    count
  }

  override private[lightdb] def applyWriteOps(ops: Seq[WriteOp[Doc]]): Task[Unit] = Task {
    val inserts = ops.collect { case WriteOp.Insert(doc) => doc }
    val upserts = ops.collect { case WriteOp.Upsert(doc) => doc }
    val deletes = ops.collect { case WriteOp.Delete(id) => id }

    if (inserts.nonEmpty) {
      val body = inserts.map(toBody).mkString("[", ",", "]")
      val result = collection.insertDocuments(RawJson.of(body))
      if (result.getErrors.asScala.exists(_.getErrorNum == DuplicateKeyErrorNum)) {
        val idx = result.getDocumentsAndErrors.asScala.indexWhere {
          case e: ErrorEntity => e.getErrorNum == DuplicateKeyErrorNum
          case _ => false
        }
        val failed = if (idx >= 0 && idx < inserts.size) inserts(idx) else inserts.head
        throw DuplicateIdException(store.name, failed._id)
      }
    }
    if (upserts.nonEmpty) {
      val body = upserts.map(toBody).mkString("[", ",", "]")
      collection.insertDocuments(RawJson.of(body), new DocumentCreateOptions().overwriteMode(OverwriteMode.replace))
    }
    if (deletes.nonEmpty) {
      val body = deletes.map(id => JsonFormatter.Compact(str(id.value))).mkString("[", ",", "]")
      collection.deleteDocuments(RawJson.of(body))
    }

    if (store.cache.isDefined) ops.foreach {
      case WriteOp.Insert(doc) => cachePending.put(doc._id, Some(doc))
      case WriteOp.Upsert(doc) => cachePending.put(doc._id, Some(doc))
      case WriteOp.Delete(id) => cachePending.put(id, None)
    }
  }

  // -- Query surface ---------------------------------------------------------------------------------

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    FilterPlanner.resolve(query.filter, store.model, resolveExistsChild = !store.supportsNativeExistsChild).flatMap { resolved =>
      val effective = if (query.optimize) resolved.map(QueryOptimizer.optimize) else resolved
      val needsInMemory = effective.exists(ArangoQuery.filterHasResidual) ||
        query.conversion.isInstanceOf[Conversion.Distance[_, _]] ||
        query.sort.exists(_.isInstanceOf[lightdb.Sort.ByDistance[_, _]])
      if (needsInMemory) executeInMemory(query, effective) else executeAql(query, effective)
    }

  private def filterPart(expr: String): String = if (expr == "true") "" else s" FILTER $expr"

  private def executeAql[V](query: Query[Doc, Model, V], effectiveFilter: Option[lightdb.filter.Filter[Doc]]): Task[SearchResults[Doc, Model, V]] = Task {
    val expr = effectiveFilter.map(f => ArangoQuery.translate(f, store.model)).getOrElse("true")
    val pageLimit = math.min(query.limit.getOrElse(Int.MaxValue), query.pageSize.getOrElse(Int.MaxValue))

    val sb = new StringBuilder("FOR d IN @@col")
    sb.append(filterPart(expr))
    ArangoQuery.sortClause(query.sort).foreach(s => sb.append(s" SORT $s"))
    if (pageLimit != Int.MaxValue) sb.append(s" LIMIT ${query.offset}, $pageLimit")
    else if (query.offset > 0) sb.append(s" LIMIT ${query.offset}, ${Int.MaxValue}")
    sb.append(" RETURN d")

    val docs = runQuery(sb.toString).map(fromRaw)
    val total = if (query.countTotal) Some(count(expr)) else None
    val withScore = docs.map(d => convertDoc(d, query.conversion) -> 0.0)
    val facetResults = if (query.facets.nonEmpty) {
      val allDocs = runQuery(s"FOR d IN @@col${filterPart(expr)} RETURN d").map(fromRaw)
      FacetComputation.fromDocs(allDocs, query.facets, store.model)
    } else Map.empty

    SearchResults(store.model, query.offset, query.limit, total, rapid.Stream.emits(withScore), facetResults, this)
  }

  private def count(expr: String): Int = {
    val raws = runQuery(s"FOR d IN @@col${filterPart(expr)} COLLECT WITH COUNT INTO n RETURN n")
    raws.headOption.map(r => JsonParser(r.get()).asInt).getOrElse(0)
  }

  /**
   * In-memory execution for predicates AQL doesn't express here (spatial Distance/Contains/Intersects,
   * hierarchical `DrillDownFacetFilter`) or distance-based ordering. Narrows the candidate set with a
   * lenient AQL superset filter, then evaluates the exact filter, ordering, radius, and facets in the
   * JVM via the shared `NestedQuerySupport.eval` / `Spatial` helpers.
   */
  private def executeInMemory[V](query: Query[Doc, Model, V], effectiveFilter: Option[lightdb.filter.Filter[Doc]]): Task[SearchResults[Doc, Model, V]] = Task {
    val broadExpr = effectiveFilter.map(f => ArangoQuery.translateLenient(f, store.model)).getOrElse("true")
    var candidates: List[Doc] = runQuery(s"FOR d IN @@col${filterPart(broadExpr)} RETURN d").map(fromRaw)
    effectiveFilter.foreach(f => candidates = candidates.filter(d => NestedQuerySupport.eval(f, store.model, d)))

    candidates = query.conversion match {
      case Conversion.Distance(field, from, sortByDistance, radius) =>
        val state = new IndexingState
        def distances(d: Doc): List[Double] = field.get(d, field, state).map(g => lightdb.spatial.Spatial.distance(from, g).valueInMeters)
        var withDist = candidates.map(d => d -> distances(d))
        radius.foreach(r => withDist = withDist.filter { case (_, ds) => ds.exists(_ <= r.valueInMeters) })
        if (sortByDistance) withDist = withDist.sortBy { case (_, ds) => ds.minOption.getOrElse(Double.MaxValue) }
        withDist.map(_._1)
      case _ => applySort(candidates, query.sort)
    }

    val total = if (query.countTotal) Some(candidates.size) else None
    val pageLimit = math.min(query.limit.getOrElse(Int.MaxValue), query.pageSize.getOrElse(Int.MaxValue))
    val dropped = if (query.offset > 0) candidates.drop(query.offset) else candidates
    val page = if (pageLimit != Int.MaxValue) dropped.take(pageLimit) else dropped
    val withScore = page.map(d => convertDoc(d, query.conversion) -> 0.0)
    val facetResults = if (query.facets.nonEmpty) FacetComputation.fromDocs(candidates, query.facets, store.model) else Map.empty

    SearchResults(store.model, query.offset, query.limit, total, rapid.Stream.emits(withScore), facetResults, this)
  }

  private def applySort(docs: List[Doc], sorts: List[lightdb.Sort]): List[Doc] =
    if (sorts.isEmpty) docs
    else {
      val state = new IndexingState
      sorts.foldRight(docs) { (s, acc) =>
        s match {
          case lightdb.Sort.IndexOrder => acc.sortBy(_._id.value)
          case bf: lightdb.Sort.ByField[Doc, _] @unchecked =>
            val field = bf.field.asInstanceOf[Field[Doc, Any]]
            val sorted = acc.sortBy(d => field.getJson(d, state))(jsonOrdering)
            if (bf.direction == lightdb.SortDirection.Descending) sorted.reverse else sorted
          case _ => acc
        }
      }
    }

  private val jsonOrdering: Ordering[Json] = Ordering.fromLessThan { (a, b) =>
    (a, b) match {
      case (NumInt(x, _), NumInt(y, _)) => x < y
      case (NumDec(x, _), NumDec(y, _)) => x < y
      case (NumInt(x, _), NumDec(y, _)) => BigDecimal(x) < y
      case (NumDec(x, _), NumInt(y, _)) => x < BigDecimal(y)
      case (Str(x, _), Str(y, _)) => x < y
      case (Null, _) => true
      case (_, Null) => false
      case _ => a.toString < b.toString
    }
  }

  private def convertDoc[V](doc: Doc, conversion: Conversion[Doc, V]): V = conversion match {
    case Conversion.Doc() => doc.asInstanceOf[V]
    case Conversion.Value(field) => field.get(doc, field, new IndexingState).asInstanceOf[V]
    case Conversion.Converted(c) => c(doc)
    case Conversion.Materialized(fields) =>
      val state = new IndexingState
      val json = obj(fields.map(f => f.name -> f.getJson(doc, state))*)
      MaterializedIndex[Doc, Model](json, store.model).asInstanceOf[V]
    case Conversion.DocAndIndexes() =>
      val state = new IndexingState
      val json = obj(store.fields.filter(_.indexed).map(f => f.name -> f.getJson(doc, state))*)
      MaterializedAndDoc[Doc, Model](json, store.model, doc).asInstanceOf[V]
    case Conversion.Json(fields) =>
      val state = new IndexingState
      obj(fields.map(f => f.name -> f.getJson(doc, state))*).asInstanceOf[V]
    case Conversion.Distance(field, from, _, _) =>
      val state = new IndexingState
      val distance = field.get(doc, field, state).map(g => lightdb.spatial.Spatial.distance(from, g))
      lightdb.spatial.DistanceAndDoc(doc, distance).asInstanceOf[V]
    case _: Conversion.DocWithInnerHits[_, _] =>
      lightdb.query.DocWithInnerHits[Doc, Model](doc = doc).asInstanceOf[V]
  }

  override def distinct[F](query: Query[Doc, Model, _],
                          field: Field[Doc, F],
                          pageSize: Int): rapid.Stream[F] = rapid.Stream.force {
    val docQuery = Query[Doc, Model, Doc](this, Conversion.Doc(), filter = query.filter, sort = query.sort)
    doSearch(docQuery).flatMap(_.stream.toList).map { docs =>
      val state = new IndexingState
      val seen = scala.collection.mutable.LinkedHashSet.empty[F]
      docs.foreach(d => seen += field.get(d, field, state))
      rapid.Stream.emits(seen.toList)
    }
  }

  // Native AQL COLLECT pushdown for the common shapes (Group + Max/Min/Avg/Sum/Count/CountDistinct,
  // no sub-aggregates, no HAVING, translatable WHERE). Everything else (sub-aggregates, Concat/
  // ConcatDistinct, HAVING, residual WHERE) falls back to the correct in-memory `Aggregator`.
  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    if canPushdownAggregate(query) then nativeAggregate(query)
    else lightdb.util.Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    aggregate(query).count

  private def canPushdownAggregate(query: AggregateQuery[Doc, Model]): Boolean =
    query.filter.isEmpty && // HAVING
      query.functions.nonEmpty &&
      !query.query.filter.exists(ArangoQuery.filterHasResidual) &&
      query.functions.forall(f => f.subAggregates.isEmpty && (f.`type` match {
        case AggregateType.Group | AggregateType.Max | AggregateType.Min | AggregateType.Avg |
             AggregateType.Sum | AggregateType.Count | AggregateType.CountDistinct => true
        case _ => false // Concat / ConcatDistinct
      }))

  private def aqlAggExpr(f: AggregateFunction[_, _, Doc]): String = {
    val field = s"d.`${ArangoQuery.escapeKey(f.field.name)}`"
    f.`type` match {
      case AggregateType.Max => s"MAX($field)"
      case AggregateType.Min => s"MIN($field)"
      case AggregateType.Avg => s"AVERAGE($field)"
      case AggregateType.Sum => s"SUM($field)"
      case AggregateType.Count => "COUNT_DISTINCT(d.`_key`)" // docs per group (keys are unique)
      case AggregateType.CountDistinct => s"COUNT_DISTINCT($field)"
      case other => throw new UnsupportedOperationException(s"Unsupported native aggregate: $other")
    }
  }

  private def nativeAggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] = rapid.Stream.fromIterator(Task {
    val groups = query.functions.filter(_.`type` == AggregateType.Group)
    val metrics = query.functions.filter(_.`type` != AggregateType.Group)
    val sb = new StringBuilder("FOR d IN @@col")
    query.query.filter.foreach(f => sb.append(s" FILTER ${ArangoQuery.translate(f, store.model)}"))
    sb.append(" COLLECT ")
    sb.append(groups.map(g => s"`${g.name}` = d.`${ArangoQuery.escapeKey(g.field.name)}`").mkString(", "))
    if (metrics.nonEmpty) {
      if (groups.nonEmpty) sb.append(" ")
      sb.append("AGGREGATE " + metrics.map(m => s"`${m.name}` = ${aqlAggExpr(m)}").mkString(", "))
    }
    if (query.sort.nonEmpty) {
      sb.append(" SORT " + query.sort.map { case (f, dir) => s"`${f.name}` ${if dir == SortDirection.Descending then "DESC" else "ASC"}" }.mkString(", "))
    }
    query.limit.foreach(l => sb.append(s" LIMIT $l"))
    val ret = query.functions.map(f => s"`${f.name}`: `${f.name}`").mkString(", ")
    sb.append(s" RETURN { $ret }")

    val cursor = store.database.query(sb.toString, classOf[RawJson], Map[String, Any]("@col" -> store.arangoName).asJava)
    new Iterator[MaterializedAggregate[Doc, Model]] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): MaterializedAggregate[Doc, Model] = MaterializedAggregate[Doc, Model](JsonParser(cursor.next().get()), store.model)
    }
  })
}
