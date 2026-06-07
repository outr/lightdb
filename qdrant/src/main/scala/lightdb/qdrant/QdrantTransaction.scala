package lightdb.qdrant

import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import _root_.io.qdrant.client.{ConditionFactory, PointIdFactory, ValueFactory, VectorsFactory, WithPayloadSelectorFactory}
import _root_.io.qdrant.client.grpc.Common.{Condition => QCondition, Filter => QFilter, PointId}
import _root_.io.qdrant.client.grpc.JsonWithInt.Value
import _root_.io.qdrant.client.grpc.Points.{PointStruct, ScrollPoints, SearchPoints}
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DuplicateIdException
import lightdb.facet.{FacetComputation, FacetResult}
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{Condition, Filter, FilterPlanner, NestedQuerySupport, QueryOptimizer}
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.store.Conversion
import lightdb.store.write.WriteOp
import lightdb.transaction.CollectionTransaction
import lightdb.{Query, SearchResults, Sort, SortDirection}
import rapid.Task

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.jdk.CollectionConverters.*

case class QdrantTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: QdrantStore[Doc, Model],
  parent: Option[lightdb.transaction.Transaction[Doc, Model]],
  writeHandlerFactory: lightdb.transaction.Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends CollectionTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private def client = store.qdrant
  private def coll = store.collectionName

  // The document's `_id` is an arbitrary string but Qdrant point ids must be UUID/uint64; derive a
  // stable UUID so get/delete are O(1) lookups.
  private def pointId(id: Id[Doc]): PointId =
    PointIdFactory.id(UUID.nameUUIDFromBytes(s"lightdb:${id.value}".getBytes(StandardCharsets.UTF_8)))

  // -- Json <-> payload ------------------------------------------------------------------------------

  private def jsonToValue(json: Json): Value = json match {
    case Str(s, _) => ValueFactory.value(s)
    case NumInt(l, _) => ValueFactory.value(l)
    case NumDec(d, _) => ValueFactory.value(d.toDouble)
    case Bool(b, _) => ValueFactory.value(b)
    case Null => ValueFactory.nullValue()
    case Arr(v, _) => ValueFactory.value(v.map(jsonToValue).asJava)
    case o: Obj => ValueFactory.value(o.value.map { case (k, v) => k -> jsonToValue(v) }.asJava)
  }

  /**
   * Payload = the full document JSON under `__doc` (for exact round-trip) plus each indexed scalar
   * field as its own key (so equality filters can push down to Qdrant).
   */
  private def toPoint(doc: Doc): PointStruct = {
    val state = new IndexingState
    val docJson = doc.json(store.model.rw)
    val payload = scala.collection.mutable.Map.empty[String, Value]
    payload.put("__doc", ValueFactory.value(JsonFormatter.Compact(docJson)))
    store.fields.filter(f => f.indexed && !f.isVector).foreach { f =>
      f.getJson(doc, state) match {
        case v @ (_: Str | _: NumInt | _: NumDec | _: Bool) => payload.put(f.name, jsonToValue(v))
        case _ => // non-scalar fields are not individually filterable; they remain in __doc
      }
    }
    val vectors = store.vectorField match {
      case Some(vf) => VectorsFactory.vectors(vf.get(doc, vf, state).map(d => java.lang.Float.valueOf(d.toFloat)).asJava)
      case None => VectorsFactory.vectors(0.0f) // placeholder for non-vector models
    }
    PointStruct.newBuilder().setId(pointId(doc._id)).setVectors(vectors).putAllPayload(payload.asJava).build()
  }

  private def fromPayload(payload: java.util.Map[String, Value]): Doc =
    JsonParser(payload.get("__doc").getStringValue).as[Doc](store.model.rw)

  // -- Filter push-down ------------------------------------------------------------------------------

  /** Translates an equality predicate on a scalar field into a Qdrant condition, when possible. */
  private def pushCondition(f: Filter[Doc]): Option[QCondition] = f match {
    case e: Filter.Equals[Doc, _] if !e.field(store.model).isVector =>
      e.getJson(store.model) match {
        case Str(s, _) => Some(ConditionFactory.matchKeyword(e.fieldName, s))
        case NumInt(l, _) => Some(ConditionFactory.`match`(e.fieldName, l))
        case Bool(b, _) => Some(ConditionFactory.`match`(e.fieldName, b))
        case _ => None
      }
    case _ => None
  }

  /** (Qdrant filter to push, whether it fully expresses the LightDB filter). */
  private def translateFilter(filter: Option[Filter[Doc]]): (Option[QFilter], Boolean) = filter match {
    case None => (None, true)
    case Some(m: Filter.Multi[Doc]) =>
      val pushed = m.filters.map(fc => if fc.condition == Condition.Must then pushCondition(fc.filter) else None)
      if pushed.nonEmpty && pushed.forall(_.isDefined) then {
        (Some(pushed.flatten.foldLeft(QFilter.newBuilder())((b, c) => b.addMust(c)).build()), true)
      } else (None, false)
    case Some(f) =>
      pushCondition(f) match {
        case Some(c) => (Some(QFilter.newBuilder().addMust(c).build()), true)
        case None => (None, false)
      }
  }

  // -- KV contract -----------------------------------------------------------------------------------

  private def existsSync(id: Id[Doc]): Boolean =
    client.retrieveAsync(coll, java.util.List.of(pointId(id)), false, false, null).get().asScala.nonEmpty

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(scrollDocs(None).iterator.map(_.json(store.model.rw))))

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      client.retrieveAsync(coll, java.util.List.of(pointId(value.asInstanceOf[Id[Doc]])), true, false, null)
        .get().asScala.headOption.map(p => fromPayload(p.getPayloadMap))
    } else {
      pushCondition(Filter.Equals[Doc, V](index.name, value)) match {
        case Some(c) => scrollDocs(Some(QFilter.newBuilder().addMust(c).build())).headOption
        case None =>
          val state = new IndexingState
          scrollDocs(None).find(d => index.get(d, index, state) == value)
      }
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    if (existsSync(doc._id)) throw DuplicateIdException(store.name, doc._id)
    client.upsertAsync(coll, java.util.List.of(toPoint(doc))).get()
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    client.upsertAsync(coll, java.util.List.of(toPoint(doc))).get()
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task(existsSync(id))

  override protected def _count: Task[Int] = Task(client.countAsync(coll).get().intValue())

  override protected def _delete(id: Id[Doc]): Task[Boolean] = Task {
    val existed = existsSync(id)
    if (existed) client.deleteAsync(coll, java.util.List.of(pointId(id))).get()
    existed
  }

  override protected def _commit: Task[Unit] = Task.unit
  override protected def _rollback: Task[Unit] = Task.unit
  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task {
    val count = client.countAsync(coll).get().intValue()
    // An empty filter matches all points.
    client.deleteAsync(coll, QFilter.newBuilder().build()).get()
    count
  }

  override private[lightdb] def applyWriteOps(ops: Seq[WriteOp[Doc]]): Task[Unit] = Task {
    if (ops.nonEmpty) {
      val upserts = ops.collect {
        case WriteOp.Insert(doc) => toPoint(doc)
        case WriteOp.Upsert(doc) => toPoint(doc)
      }
      val deletes = ops.collect { case WriteOp.Delete(id) => pointId(id) }
      if (upserts.nonEmpty) client.upsertAsync(coll, upserts.asJava).get()
      if (deletes.nonEmpty) client.deleteAsync(coll, deletes.asJava).get()
      if (store.cache.isDefined) ops.foreach {
        case WriteOp.Insert(doc) => cachePending.put(doc._id, Some(doc))
        case WriteOp.Upsert(doc) => cachePending.put(doc._id, Some(doc))
        case WriteOp.Delete(id) => cachePending.put(id, None)
      }
    }
  }

  // -- Query surface ---------------------------------------------------------------------------------

  private def scrollDocs(filter: Option[QFilter]): List[Doc] = {
    val buffer = scala.collection.mutable.ListBuffer.empty[Doc]
    var offset: Option[PointId] = None
    var continue = true
    while (continue) {
      val b = ScrollPoints.newBuilder()
        .setCollectionName(coll)
        .setLimit(256)
        .setWithPayload(WithPayloadSelectorFactory.enable(true))
      filter.foreach(b.setFilter)
      offset.foreach(b.setOffset)
      val response = client.scrollAsync(b.build()).get()
      response.getResultList.asScala.foreach(p => buffer += fromPayload(p.getPayloadMap))
      if (response.hasNextPageOffset) offset = Some(response.getNextPageOffset) else continue = false
    }
    buffer.toList
  }

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    FilterPlanner.resolve(query.filter, store.model, resolveExistsChild = !store.supportsNativeExistsChild).flatMap { resolved =>
      val effective = if (query.optimize) resolved.map(QueryOptimizer.optimize) else resolved
      query.sort.collectFirst { case bv: Sort.ByVectorDistance[_] => bv } match {
        case Some(bv) => executeKnn(query, effective, bv.asInstanceOf[Sort.ByVectorDistance[Doc]])
        case None => executeScroll(query, effective)
      }
    }

  private def executeKnn[V](query: Query[Doc, Model, V],
                            effective: Option[Filter[Doc]],
                            bv: Sort.ByVectorDistance[Doc]): Task[SearchResults[Doc, Model, V]] = Task {
    if (store.vectorField.isEmpty) throw new UnsupportedOperationException(s"Vector search requires a vector field on '${store.name}'")
    if (bv.direction == SortDirection.Descending) throw new UnsupportedOperationException(
      "Qdrant vector search returns nearest-first only; a descending Sort.ByVectorDistance is not supported."
    )
    val (qFilter, fullyPushed) = translateFilter(effective)
    val want = query.offset + query.limit.orElse(query.pageSize).getOrElse(100)
    // When the filter can't be fully pushed, over-fetch so the in-memory filter still yields enough.
    val fetch = if (fullyPushed) want else math.min(want * 10 + 50, 10_000)
    val vector = bv.vector.map(d => java.lang.Float.valueOf(d.toFloat)).asJava
    val sb = SearchPoints.newBuilder()
      .setCollectionName(coll)
      .addAllVector(vector)
      .setLimit(fetch.toLong)
      .setWithPayload(WithPayloadSelectorFactory.enable(true))
    qFilter.foreach(sb.setFilter)
    var docs = client.searchAsync(sb.build()).get().asScala.toList.map(p => fromPayload(p.getPayloadMap))
    if (!fullyPushed) effective.foreach(f => docs = docs.filter(d => NestedQuerySupport.eval(f, store.model, d)))
    finish(query, docs, ordered = true)
  }

  private def executeScroll[V](query: Query[Doc, Model, V],
                               effective: Option[Filter[Doc]]): Task[SearchResults[Doc, Model, V]] = Task {
    val (qFilter, fullyPushed) = translateFilter(effective)
    var candidates = scrollDocs(qFilter)
    if (!fullyPushed) effective.foreach(f => candidates = candidates.filter(d => NestedQuerySupport.eval(f, store.model, d)))
    candidates = applySort(candidates, query.sort)
    finish(query, candidates, ordered = true)
  }

  /** Shared paging/conversion/faceting over an already-ordered candidate list. */
  private def finish[V](query: Query[Doc, Model, V], candidates: List[Doc], ordered: Boolean): SearchResults[Doc, Model, V] = {
    val total = if (query.countTotal) Some(candidates.size) else None
    val pageLimit = math.min(query.limit.getOrElse(Int.MaxValue), query.pageSize.getOrElse(Int.MaxValue))
    val dropped = if (query.offset > 0) candidates.drop(query.offset) else candidates
    val page = if (pageLimit != Int.MaxValue) dropped.take(pageLimit) else dropped
    val withScore = page.map(d => convertDoc(d, query.conversion) -> 0.0)
    val facetResults: Map[Field.FacetField[Doc], FacetResult] =
      if (query.facets.nonEmpty) FacetComputation.fromDocs(candidates, query.facets, store.model)
      else Map.empty[Field.FacetField[Doc], FacetResult]
    SearchResults(store.model, query.offset, query.limit, total, rapid.Stream.emits(withScore), facetResults, this)
  }

  private def applySort(docs: List[Doc], sorts: List[Sort]): List[Doc] =
    if (sorts.isEmpty) docs
    else {
      val state = new IndexingState
      sorts.foldRight(docs) { (s, acc) =>
        s match {
          case Sort.IndexOrder => acc.sortBy(_._id.value)
          case bf: Sort.ByField[Doc, _] @unchecked =>
            val field = bf.field.asInstanceOf[Field[Doc, Any]]
            val sorted = acc.sortBy(d => field.getJson(d, state))(jsonOrdering)
            if (bf.direction == SortDirection.Descending) sorted.reverse else sorted
          case _: Sort.ByVectorDistance[_] =>
            throw new UnsupportedOperationException("Vector sort must be the primary sort (handled natively); cannot combine with other sorts here.")
          case _: Sort.ByDistance[_, _] =>
            throw new UnsupportedOperationException(
              "Spatial Sort.ByDistance is not supported by the Qdrant backend; use a backend with native spatial support."
            )
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

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    lightdb.util.Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = aggregate(query).count
}
