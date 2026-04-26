package lightdb.tantivy

import fabric.*
import fabric.io.JsonFormatter
import fabric.rw.*
import lightdb.aggregate.{AggregateFunction, AggregateQuery, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.{FacetComputation, FacetResult, FacetResultValue}
import lightdb.field.Field
import lightdb.field.Field.FacetField
import lightdb.filter.NestedQuerySupport
import lightdb.id.Id
import lightdb.materialized.{MaterializedAggregate, MaterializedIndex}
import lightdb.{Query, SearchResults, Sort}
import lightdb.store.Conversion
import lightdb.transaction.{CollectionTransaction, RollbackSupport, Transaction, WriteHandler}
import rapid.*
import scantivy.proto as pb

case class TantivyTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: TantivyStore[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]
) extends CollectionTransaction[Doc, Model]
    with RollbackSupport[Doc, Model] {

  override lazy val writeHandler: WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private def index = store.tantivyIndex

  // -- abstract Transaction overrides ---------------------------------------------------------

  override protected def _get[V](idxField: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    val q = pb.Query(pb.Query.Node.Term(pb.QueryTerm(
      field = idxField.name,
      value = Some(TantivyValue.fromAny(idxField, value))
    )))
    val req = pb.SearchRequest(query = Some(q), limit = 1, conversion = pb.ConversionMode.CONVERSION_DOC)
    index.search(req).fold(e => throw new RuntimeException(e), identity)
      .hits
      .headOption
      .flatMap(_.payload.doc)
      .map(d => TantivyDocConvert.fromPb(store.model, d))
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    val pbDoc = TantivyDocConvert.toPb(store.model, store.fields, doc)
    index.index(pbDoc).fold(e => throw new RuntimeException(e), _ => doc)
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val pbDoc = TantivyDocConvert.toPb(store.model, store.fields, doc)
    val idValue = TantivyValue.fromAny(store.idField, doc._id)
    index.upsert(idValue, pbDoc).fold(e => throw new RuntimeException(e), _ => doc)
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = _get(store.idField, id).map(_.nonEmpty)

  override protected def _count: Task[Int] = Task {
    index.count(TantivyFilter.allQuery).fold(e => throw new RuntimeException(e), identity).toInt
  }

  override protected def _delete(id: Id[Doc]): Task[Boolean] = Task {
    val idValue = TantivyValue.fromAny(store.idField, id)
    index.delete(idValue).fold(e => throw new RuntimeException(e), _ => true)
  }

  override protected def _commit: Task[Unit] = Task {
    index.commit().fold(e => throw new RuntimeException(e), _ => ())
  }

  override protected def _rollback: Task[Unit] = Task {
    index.rollback().fold(e => throw new RuntimeException(e), _ => ())
  }

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task {
    val n = index.count(TantivyFilter.allQuery).getOrElse(0L).toInt
    index.truncate().fold(e => throw new RuntimeException(e), _ => ())
    index.commit().fold(e => throw new RuntimeException(e), _ => ())
    n
  }

  override def jsonStream: rapid.Stream[Json] = {
    rapid.Stream.force(doSearch[Json](Query[Doc, Model, Json](this, Conversion.Json(store.fields))).map(_.stream))
  }

  // -- CollectionTransaction overrides --------------------------------------------------------

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = Task {
    if NestedQuerySupport.containsNested(query.filter) then doSearchWithNestedFallback(query)
    else doSearchDirect(query)
  }

  private def doSearchDirect[V](query: Query[Doc, Model, V]): SearchResults[Doc, Model, V] = {
    val pbQuery = TantivyFilter.compile(store.model, query.filter)
    val sortClauses = query.sort.map(TantivySort.compile)
    val pageSize = query.limit.orElse(query.pageSize).getOrElse(100_000_000)
    val pbFacetRequests = query.facets.map(fq => pb.FacetRequest(
      field = fq.field.name,
      path = fq.path,
      childrenLimit = fq.childrenLimit,
      dimsLimit = fq.dimsLimit
    ))
    val req = pb.SearchRequest(
      query = Some(pbQuery),
      sort = sortClauses,
      offset = query.offset,
      limit = pageSize,
      countTotal = query.countTotal,
      scoreDocs = query.scoreDocs,
      minDocScore = query.minDocScore,
      facets = pbFacetRequests,
      conversion = pb.ConversionMode.CONVERSION_DOC
    )
    val resp = index.search(req).fold(e => throw new RuntimeException(e), identity)
    val facetResultMap: Map[FacetField[Doc], FacetResult] = query.facets.flatMap { fq =>
      resp.facets.find(_.field == fq.field.name).map { fr =>
        // Scantivy 1.0 returns facet entry labels as the full path from root (e.g. `/2010` for
        // a year-level child or `/2010/10/15` for a day-level leaf). LightDB's Lucene
        // convention is to return labels RELATIVE to the queried path (`10` if the query was
        // at `/2010`, `15` at `/2010/10`). Strip both the leading `/` and the requested-path
        // prefix.
        // Hierarchical facets also carry a `$ROOT$` sentinel for the "stops at this level"
        // bucket — keep it counted in `childCount` but exclude it from `entries` and
        // `totalCount`.
        val sentinel = TantivyDocConvert.FacetRootSentinel
        val prefix =
          if fq.path.isEmpty then ""
          else fq.path.mkString("", "/", "/")
        def relativize(label: String): String = {
          val noLead = if label.startsWith("/") then label.substring(1) else label
          if prefix.nonEmpty && noLead.startsWith(prefix) then noLead.substring(prefix.length)
          else noLead
        }
        val cleaned = fr.entries.toList.map(e => (relativize(e.label), e.count))
        val (rootEntries, namedEntries) = cleaned.partition(_._1 == sentinel)
        val values = namedEntries.map { case (label, count) => FacetResultValue(label, count) }
        val rootCount = rootEntries.map(_._2).sum
        val adjustedTotal = (fr.totalCount.toInt - rootCount.toInt).max(0)
        fq.field -> FacetResult(values, fr.childCount, adjustedTotal)
      }
    }.toMap

    val pairs: List[(V, Double)] = resp.hits.toList.map { hit =>
      val pbDoc = hit.payload.doc.getOrElse(throw new RuntimeException("search hit missing doc payload"))
      val doc = TantivyDocConvert.fromPb(store.model, pbDoc)
      val score = hit.score.map(_.toDouble).getOrElse(0.0)
      val v: V = convertHit(doc, query)
      v -> score
    }

    val streamWithScore = rapid.Stream.fromIterator(Task(pairs.iterator))
    SearchResults(
      model = store.model,
      offset = query.offset,
      limit = query.limit,
      total = resp.total.map(_.toInt),
      streamWithScore = streamWithScore,
      facetResults = facetResultMap,
      transaction = this
    )
  }

  private def convertHit[V](doc: Doc, query: Query[Doc, Model, V]): V = (query.conversion match {
    case Conversion.Doc() => doc
    case Conversion.Value(field) => field.get(doc, field, new lightdb.field.IndexingState)
    case Conversion.Converted(c) => c(doc)
    case Conversion.Json(fields) =>
      val pairs = fields.map { f =>
        val state = new lightdb.field.IndexingState
        f.name -> f.getJson(doc, state)
      }
      obj(pairs*)
    case Conversion.Materialized(fields) =>
      val pairs = fields.map { f =>
        val state = new lightdb.field.IndexingState
        f.name -> f.getJson(doc, state)
      }
      MaterializedIndex[Doc, Model](obj(pairs*), store.model)
    case Conversion.DocAndIndexes() =>
      val pairs = store.model.indexedFields.map { f =>
        val state = new lightdb.field.IndexingState
        f.name -> f.getJson(doc, state)
      }
      lightdb.materialized.MaterializedAndDoc[Doc, Model](obj(pairs*), store.model, doc)
    case _: Conversion.Distance[Doc, _] =>
      throw new UnsupportedOperationException("Tantivy backend does not support Conversion.Distance — no native geo support.")
  }).asInstanceOf[V]

  /** Run a broad query (with nested constraints stripped), then post-filter results in-memory
   *  via `TantivyNestedEval`. Mirrors Lucene's nested-fallback pattern. Facets are computed
   *  in-memory from the post-filtered docs via `FacetComputation.fromDocs`.
   *
   *  This is the only fallback the Tantivy backend supports — and it matches LightDB's standard
   *  `NestedQueryCapability.Fallback` contract that other backends use too. Other "Tantivy
   *  doesn't support this natively" cases (geo, prefix-scan over `_id`, etc.) deliberately do
   *  NOT get an in-memory fallback because that would silently turn a O(matches) operation
   *  into a full collection scan and mislead callers about real-world performance.
   */
  private def doSearchWithNestedFallback[V](query: Query[Doc, Model, V]): SearchResults[Doc, Model, V] = {
    NestedQuerySupport.validateFallbackCompatible(query.filter)
    val originalFilter = query.filter
    val broadFilter = NestedQuerySupport.stripNested(originalFilter)

    val docOnly = Query[Doc, Model, Doc](this, Conversion.Doc()).copy(
      filter = broadFilter,
      sort = query.sort,
      offset = 0,
      limit = None,
      pageSize = None,
      countTotal = false,
      facets = Nil
    )
    val direct = doSearchDirect(docOnly)
    val all = direct.streamWithScore.toList.sync()
    val matched0 = all.filter { case (doc, _) =>
      // Use the Tantivy-local evaluator for block-level must_not semantics inside nested
      // filters (matches Lucene's block-join behavior; the shared NestedQuerySupport.eval
      // applies must_not per-element).
      originalFilter.forall(f => TantivyNestedEval.eval(f, store.model, doc))
    }
    val matched =
      if query.sort.isEmpty || query.sort.exists(_ == lightdb.Sort.IndexOrder) then
        matched0.sortBy(_._1._id.value)
      else matched0
    val total = matched.size
    val limited = matched.drop(query.offset).take(query.limit.orElse(query.pageSize).getOrElse(Int.MaxValue))
    val converted = limited.map { case (doc, score) => convertHit(doc, query) -> score }
    val facetResultMap: Map[FacetField[Doc], FacetResult] =
      if query.facets.isEmpty then Map.empty
      else FacetComputation.fromDocs(matched.map(_._1), query.facets, store.model)

    SearchResults(
      model = store.model,
      offset = query.offset,
      limit = query.limit,
      total = if query.countTotal then Some(total) else None,
      streamWithScore = rapid.Stream.fromIterator(Task(converted.iterator)),
      facetResults = facetResultMap,
      transaction = this
    )
  }

  // -- Aggregations ---------------------------------------------------------------------------

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    rapid.Stream.force(Task {
      // Split aggregate functions into the ones we can ship to Tantivy's native aggregation
      // framework (numeric metrics) and the ones we have to compute by streaming the docs
      // (Concat/ConcatDistinct/Group — Tantivy has no direct equivalent).
      val (native, streamed) = query.functions.partition(isNativeAgg)

      val nativePairs: Map[String, Json] =
        if native.isEmpty then Map.empty
        else {
          val req = buildAggRequest(query, native)
          val resp = index.aggregate(req).fold(e => throw new RuntimeException(e), identity)
          val byAlias = resp.results.map(r => r.alias -> r).toMap
          native.map { fn =>
            val alias = aggAlias(fn)
            val v = byAlias.get(alias).flatMap(extractNumeric)
            alias -> v.map(NumDec(_)).getOrElse(Null)
          }.toMap
        }

      val streamedPairs: Map[String, Json] =
        if streamed.isEmpty then Map.empty
        else streamingAgg(query, streamed)

      val pairs = query.functions.map { fn =>
        val alias = aggAlias(fn)
        alias -> nativePairs.getOrElse(alias, streamedPairs.getOrElse(alias, Null))
      }
      val mat = MaterializedAggregate[Doc, Model](obj(pairs*), store.model)
      rapid.Stream.emit(mat)
    })

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = Task {
    val pbQuery = TantivyFilter.compile(store.model, query.query.filter)
    index.count(pbQuery).fold(e => throw new RuntimeException(e), identity).toInt
  }

  /** Distinct values for a field. If the filter contains nested predicates we can't ship to
   *  Tantivy, fall back to streaming docs and dedup'ing in memory.
   */
  override def distinct[F](query: Query[Doc, Model, ?], field: Field[Doc, F], pageSize: Int): rapid.Stream[F] = {
    if NestedQuerySupport.containsNested(query.filter) then {
      // Stream matching docs through the nested-fallback path, then extract field values uniquely.
      val docQuery = Query[Doc, Model, Doc](this, Conversion.Doc()).copy(
        filter = query.filter,
        sort = query.sort,
        offset = 0,
        limit = None,
        pageSize = None
      )
      rapid.Stream.force(Task {
        val results = doSearch(docQuery).sync()
        val state = new lightdb.field.IndexingState
        val seen = scala.collection.mutable.LinkedHashSet.empty[F]
        results.streamWithScore.toList.sync().foreach { case (doc, _) =>
          val v = field.get(doc, field, state)
          seen += v
        }
        rapid.Stream.emits(seen.toList)
      })
    } else {
      // Native composite-cursor paging via the Scantivy FFI.
      val pbFilter = query.filter.map(_ => TantivyFilter.compile(store.model, query.filter))
      def page(cursor: Option[scantivy.proto.Value]): rapid.Stream[F] = rapid.Stream.force(Task {
        val req = scantivy.proto.DistinctRequest(
          indexId = "",
          filterQuery = pbFilter,
          field = field.name,
          pageSize = pageSize,
          cursor = cursor
        )
        val resp = index.distinctPage(req).fold(e => throw new RuntimeException(e), identity)
        val values = resp.values.toList.flatMap { v =>
          val json = TantivyValue.toJson(v)
          scala.util.Try(json.as[F](field.rw)).toOption
        }
        val head = rapid.Stream.emits(values)
        resp.nextCursor match {
          case Some(c) => head.append(page(Some(c)))
          case None => head
        }
      })
      page(None)
    }
  }

  private def buildAggRequest(
    query: AggregateQuery[Doc, Model],
    fns: List[AggregateFunction[?, ?, Doc]]
  ): pb.AggregationRequest = {
    val filterQuery = query.query.filter.map(_ => TantivyFilter.compile(store.model, query.query.filter))
    pb.AggregationRequest(filterQuery = filterQuery, functions = fns.map(toPbFunction))
  }

  private def aggAlias(fn: AggregateFunction[?, ?, Doc]): String = fn.name

  private def isNativeAgg(fn: AggregateFunction[?, ?, Doc]): Boolean = fn.`type` match {
    case AggregateType.Sum | AggregateType.Avg | AggregateType.Min | AggregateType.Max
       | AggregateType.Count | AggregateType.CountDistinct => true
    case _ => false
  }

  private def toPbFunction(fn: AggregateFunction[?, ?, Doc]): pb.AggregationFunction = {
    val ty: pb.AggregationType = fn.`type` match {
      case AggregateType.Sum => pb.AggregationType.AGG_SUM
      case AggregateType.Avg => pb.AggregationType.AGG_AVG
      case AggregateType.Min => pb.AggregationType.AGG_MIN
      case AggregateType.Max => pb.AggregationType.AGG_MAX
      case AggregateType.Count => pb.AggregationType.AGG_COUNT
      case AggregateType.CountDistinct => pb.AggregationType.AGG_CARDINALITY
      case other =>
        throw new UnsupportedOperationException(s"Tantivy backend does not support aggregate type: $other")
    }
    pb.AggregationFunction(alias = aggAlias(fn), field = fn.field.name, `type` = ty)
  }

  private def extractNumeric(r: pb.AggregationResult): Option[Double] = r.value match {
    case pb.AggregationResult.Value.Numeric(d) => Some(d)
    case _ => None
  }

  /** Compute Concat/ConcatDistinct/Group by streaming the matching docs and collecting the field
   *  values per aggregate. Tantivy lacks a native equivalent, so we fall back to a single scan.
   *  Results are sorted by `_id` so the output order is deterministic across runs.
   */
  private def streamingAgg(
    query: AggregateQuery[Doc, Model],
    fns: List[AggregateFunction[?, ?, Doc]]
  ): Map[String, Json] = {
    val docs = query.query.docs.toList.sync().sortBy(_._id.value)
    val state = new lightdb.field.IndexingState
    fns.map { fn =>
      val values: List[Json] = docs.map(d => fn.field.getJson(d, state))
      val out: Json = fn.`type` match {
        case AggregateType.Concat => Arr(values.toVector)
        case AggregateType.ConcatDistinct => Arr(values.distinct.toVector)
        case AggregateType.Group => Arr(values.distinct.toVector)
        case _ => Null
      }
      aggAlias(fn) -> out
    }.toMap
  }
}
