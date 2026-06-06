package lightdb.mongodb

import com.mongodb.{MongoBulkWriteException, MongoWriteException}
import com.mongodb.client.model.{BulkWriteOptions, DeleteOneModel, Filters, InsertOneModel, ReplaceOneModel, ReplaceOptions, WriteModel}
import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.DuplicateIdException
import lightdb.facet.FacetComputation
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{FilterPlanner, NestedQuerySupport, QueryOptimizer}
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedAndDoc, MaterializedIndex}
import lightdb.store.Conversion
import lightdb.store.write.WriteOp
import lightdb.transaction.CollectionTransaction
import lightdb.{Query, SearchResults}
import org.bson.Document as BsonDoc
import org.bson.conversions.Bson
import rapid.Task

import scala.jdk.CollectionConverters.*

case class MongoDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: MongoDBStore[Doc, Model],
  parent: Option[lightdb.transaction.Transaction[Doc, Model]],
  writeHandlerFactory: lightdb.transaction.Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends CollectionTransaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private def collection = store.collection

  // -- Json <-> BSON ---------------------------------------------------------------------------------

  /**
   * Stored BSON = the document's real fields (from `doc.json`) merged with every indexed-field
   * projection. The projections make computed indexed fields (e.g. `allNames`, `ageDouble`) and
   * tokenized fields queryable/sortable even though they aren't part of the case class. Tokenized
   * fields are stored as token arrays so per-term matching works via MongoDB's array semantics.
   * On read, `as[Doc]` ignores the extra keys and reconstructs from the real fields.
   */
  private[mongodb] def toBson(doc: Doc): BsonDoc = {
    val state = new IndexingState
    val base: Map[String, Json] = doc.json(store.model.rw) match {
      case o: Obj => o.value
      case _ => Map.empty
    }
    val merged = store.fields.foldLeft(base) { (m, f) =>
      val raw = f.getJson(doc, state)
      val stored = if f.isTokenized then raw match {
        case Str(s, _) => arr(MongoQuery.tokenize(s).map(str)*)
        case other => other
      } else raw
      m.updated(f.name, stored)
    }
    BsonDoc.parse(JsonFormatter.Compact(Obj(merged)))
  }

  private def fromBson(bson: BsonDoc): Doc = JsonParser(bson.toJson).as[Doc](store.model.rw)

  private def idFilter(id: Id[Doc]): Bson = Filters.eq("_id", id.value)

  // -- KV contract -----------------------------------------------------------------------------------

  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    collection.find().iterator().asScala.map(b => JsonParser(b.toJson))
  })

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if index == store.idField then {
      Option(collection.find(idFilter(value.asInstanceOf[Id[Doc]])).first()).map(fromBson)
    } else {
      Option(collection.find(Filters.eq(index.name, MongoQuery.jsonToBson(index.rw.read(value)))).first()).map(fromBson)
    }
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    collection.replaceOne(idFilter(doc._id), toBson(doc), new ReplaceOptions().upsert(true))
    doc
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    try {
      collection.insertOne(toBson(doc))
      doc
    } catch {
      case e: MongoWriteException if e.getError.getCode == 11000 =>
        throw DuplicateIdException(store.name, doc._id)
    }
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task(collection.countDocuments(idFilter(id)) > 0L)

  override protected def _count: Task[Int] = Task(collection.countDocuments().toInt)

  override protected def _delete(id: Id[Doc]): Task[Boolean] = Task(collection.deleteOne(idFilter(id)).getDeletedCount > 0L)

  override protected def _commit: Task[Unit] = Task.unit
  override protected def _rollback: Task[Unit] = Task.unit
  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task(collection.deleteMany(Filters.empty()).getDeletedCount.toInt)

  /** Native bulk path — see Phase 1 notes. Mirrors the private `Transaction.trackCacheWrite`. */
  override private[lightdb] def applyWriteOps(ops: Seq[WriteOp[Doc]]): Task[Unit] = Task {
    if (ops.nonEmpty) {
      val models: java.util.List[WriteModel[BsonDoc]] = ops.map[WriteModel[BsonDoc]] {
        case WriteOp.Insert(doc) => new InsertOneModel[BsonDoc](toBson(doc))
        case WriteOp.Upsert(doc) => new ReplaceOneModel[BsonDoc](idFilter(doc._id), toBson(doc), new ReplaceOptions().upsert(true))
        case WriteOp.Delete(id) => new DeleteOneModel[BsonDoc](idFilter(id))
      }.asJava
      try {
        collection.bulkWrite(models, new BulkWriteOptions().ordered(true))
      } catch {
        case e: MongoBulkWriteException =>
          e.getWriteErrors.asScala.find(_.getCode == 11000) match {
            case Some(we) => throw DuplicateIdException(store.name, ops(we.getIndex).id)
            case None => throw e
          }
      }
      if (store.cache.isDefined) ops.foreach {
        case WriteOp.Insert(doc) => cachePending.put(doc._id, Some(doc))
        case WriteOp.Upsert(doc) => cachePending.put(doc._id, Some(doc))
        case WriteOp.Delete(id) => cachePending.put(id, None)
      }
    }
  }

  // -- Query surface ---------------------------------------------------------------------------------

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    // Resolve parent/child `ExistsChild` filters into id-based parent filters (no native join in
    // MongoDB), then optimize, before translating to BSON.
    FilterPlanner.resolve(query.filter, store.model, resolveExistsChild = !store.supportsNativeExistsChild).flatMap { resolved =>
      val effective = if (query.optimize) resolved.map(QueryOptimizer.optimize) else resolved
      val needsInMemory = effective.exists(MongoQuery.filterHasResidual) ||
        query.conversion.isInstanceOf[Conversion.Distance[_, _]] ||
        query.sort.exists(_.isInstanceOf[lightdb.Sort.ByDistance[_, _]])
      if (needsInMemory) executeInMemory(query, effective) else executeSearch(query, effective)
    }

  private def executeSearch[V](query: Query[Doc, Model, V], effectiveFilter: Option[lightdb.filter.Filter[Doc]]): Task[SearchResults[Doc, Model, V]] = Task {
    val filterBson: Bson = effectiveFilter.map(f => MongoQuery.translate(f, store.model)).getOrElse(Filters.empty())
    var find = collection.find(filterBson)
    MongoQuery.sortDoc(query.sort).foreach(s => find = find.sort(s))
    if (query.offset > 0) find = find.skip(query.offset)
    // A single doSearch returns one page: bounded by pageSize, and never more than an explicit limit.
    // The default `streamScored` pages by `pageSize`, so each page must cap at pageSize to avoid overlap.
    val pageLimit = math.min(query.limit.getOrElse(Int.MaxValue), query.pageSize.getOrElse(Int.MaxValue))
    if (pageLimit != Int.MaxValue) find = find.limit(pageLimit)

    val docs = find.iterator().asScala.map(fromBson).toList
    val total = if (query.countTotal) Some(collection.countDocuments(filterBson).toInt) else None
    val withScore = docs.map(d => convertDoc(d, query.conversion) -> 0.0)

    // Facets are computed in-memory over ALL matching docs (not just this page), mirroring the SQL
    // backend's FacetComputation.fromDocs approach.
    val facetResults = if (query.facets.nonEmpty) {
      val allDocs = collection.find(filterBson).iterator().asScala.map(fromBson).to(Iterable)
      FacetComputation.fromDocs(allDocs, query.facets, store.model)
    } else Map.empty

    SearchResults(
      model = store.model,
      offset = query.offset,
      limit = query.limit,
      total = total,
      streamWithScore = rapid.Stream.emits(withScore),
      facetResults = facetResults,
      transaction = this
    )
  }

  /**
   * In-memory execution for predicates MongoDB can't express natively (spatial Distance/Contains/
   * Intersects, hierarchical `DrillDownFacetFilter`) or distance-based ordering. Narrows the candidate
   * set with a lenient BSON superset filter, then evaluates the exact filter, ordering, radius, and
   * facets in the JVM via the shared `NestedQuerySupport.eval` / `Spatial` helpers.
   */
  private def executeInMemory[V](query: Query[Doc, Model, V], effectiveFilter: Option[lightdb.filter.Filter[Doc]]): Task[SearchResults[Doc, Model, V]] = Task {
    val broadBson: Bson = effectiveFilter.map(f => MongoQuery.translateLenient(f, store.model)).getOrElse(Filters.empty())
    var candidates: List[Doc] = collection.find(broadBson).iterator().asScala.map(fromBson).toList
    effectiveFilter.foreach(f => candidates = candidates.filter(d => NestedQuerySupport.eval(f, store.model, d)))

    // A Distance conversion drives radius filtering + distance ordering; otherwise apply query.sort.
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

  // Aggregation is computed in-memory via the shared `Aggregator` (streams matched docs and folds
  // them), which is correct for all AggregateType/Group/sub-aggregate/Concat cases. A native
  // `$group`-pipeline pushdown is a possible future performance optimization.
  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    lightdb.util.Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    aggregate(query).count
}
