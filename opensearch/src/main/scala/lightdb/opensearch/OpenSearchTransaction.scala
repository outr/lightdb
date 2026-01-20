package lightdb.opensearch

import fabric._
import fabric.io.JsonFormatter
import fabric.io.JsonParser
import fabric.rw._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.{FacetQuery, FacetValue}
import lightdb.filter.{FilterPlanner, QueryOptimizer}
import lightdb.field.Field.FacetField
import lightdb.field.Field.UniqueIndex
import lightdb.field.FieldAndValue
import lightdb.field.IndexingState
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.opensearch.util.OpenSearchCursor
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.opensearch.query.{OpenSearchDsl, OpenSearchSearchBuilder}
import lightdb.spatial.{DistanceAndDoc, Geo, Spatial}
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction, Transaction}
import lightdb.store.{BufferedWritingTransaction, Conversion, StoreMode, WriteOp}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.{Query, SearchResults, Sort}
import lightdb.util.Aggregator
import lightdb.opensearch.util.OpenSearchCursor
import rapid.{Task, logger}
import rapid.taskSeq2Ops

import java.util.concurrent.Semaphore
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import lightdb.opensearch.OpenSearchMetrics
import rapid.Unique

case class OpenSearchTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: OpenSearchStore[Doc, Model],
                                                                                    config: OpenSearchConfig,
                                                                                    client: OpenSearchClient,
                                                                                    parent: Option[Transaction[Doc, Model]])
  extends CollectionTransaction[Doc, Model]
    with PrefixScanningTransaction[Doc, Model]
    with BufferedWritingTransaction[Doc, Model] {
  @volatile private var commitResult: Option[Try[Unit]] = None
  @volatile private var refreshPolicyOverride: Option[Option[String]] = None
  @volatile private var wroteToIndex: Boolean = false
  // A best-effort "searchable barrier" for strict read-after-commit semantics:
  // we remember a recently indexed document id and confirm it is visible via a search API after refresh.
  @volatile private var lastIndexedIdForSearchBarrier: Option[String] = None
  @volatile private var refreshIntervalOverride: Option[String] = None
  @volatile private var restoreRefreshIntervalOnClose: Boolean = false
  @volatile private var originalRefreshInterval: Option[String] = None
  @volatile private var refreshIntervalApplied: Boolean = false

  /**
   * Default OpenSearch transaction semantics prioritize ingest throughput.
   *
   * We intentionally do NOT flush pending writes before reads/queries, meaning "read-your-writes" is not guaranteed
   * within an active transaction. Visibility is guaranteed at transaction boundaries by an explicit index refresh
   * during commit.
   */
  private def beforeRead: Task[Unit] =
    // Still apply any index-level tuning (ex: refresh_interval) before issuing read APIs.
    applyIndexTuningIfNeeded

  private def applyIndexTuningIfNeeded: Task[Unit] = Task.defer {
    refreshIntervalOverride match {
      case None => Task.unit
      case Some(interval) if refreshIntervalApplied => Task.unit
      case Some(interval) =>
        val index = store.writeIndexName
        val loadOriginal: Task[Unit] =
          if (restoreRefreshIntervalOnClose && originalRefreshInterval.isEmpty) {
            client.indexSettings(index, flatSettings = true, includeDefaults = true).map { json =>
              originalRefreshInterval = extractRefreshInterval(index, json)
            }
          } else {
            Task.unit
          }
        val apply: Task[Unit] =
          client.updateIndexSettings(
            index,
            obj("index" -> obj("refresh_interval" -> str(interval)))
          ).map { _ =>
            refreshIntervalApplied = true
          }
        loadOriginal.next(apply)
    }
  }

  private def restoreIndexTuningIfNeeded: Task[Unit] = Task.defer {
    if (!restoreRefreshIntervalOnClose) {
      Task.unit
    } else {
      originalRefreshInterval match {
        case None => Task.unit
        case Some(orig) =>
          val index = store.writeIndexName
          client.updateIndexSettings(
            index,
            obj("index" -> obj("refresh_interval" -> str(orig)))
          ).attempt.flatMap {
            case Success(_) => Task.unit
            case Failure(t) =>
              Task(scribe.warn(s"Failed to restore OpenSearch refresh_interval for $index: ${t.getMessage}"))
          }
      }
    }
  }

  private def extractRefreshInterval(index: String, settingsResponse: Json): Option[String] = {
    // OpenSearch returns: { "<index>": { "settings": { "index.refresh_interval": "1s", ... } } } when flat_settings=true.
    // Some versions may return nested: { "<index>": { "settings": { "index": { "refresh_interval": "1s" } } } }.
    val root = settingsResponse.asObj
    val idx = root.get(index).orElse(root.value.valuesIterator.toSeq.headOption).map(_.asObj)
    val settings = idx.flatMap(_.get("settings")).map(_.asObj)
    val flat = settings.flatMap(_.get("index.refresh_interval")).map(_.asStr.value)
    val nested = settings
      .flatMap(_.get("index"))
      .map(_.asObj)
      .flatMap(_.get("refresh_interval"))
      .map(_.asStr.value)
    flat.orElse(nested)
  }

  private def flushPendingWrites: Task[Unit] =
    withWriteBuffer { wb =>
      if (wb.map.nonEmpty) flushMap(wb.map) else Task.pure(wb)
    }.unit

  override protected def _commit: Task[Unit] = commitResult match {
    case Some(Success(_)) => Task.unit
    case Some(Failure(t)) => Task.error(t)
    case None =>
      // Commit buffered writes, then (if we wrote) force a refresh so documents are searchable immediately after the
      // LightDB transaction completes (without per-bulk wait_for overhead).
      def awaitSearchableBarrier: Task[Unit] = lastIndexedIdForSearchBarrier match {
        case None => Task.unit
        case Some(id) =>
          // If the index was just recreated (common in reIndex), OpenSearch can report refresh success but still
          // not immediately return hits for the freshly written docs. We guard against that by polling a cheap
          // count query on _id until it becomes visible.
          val query = obj("query" -> obj("ids" -> obj("values" -> arr(str(id)))))
          def loop(attempt: Int, delay: FiniteDuration): Task[Unit] = {
            client.count(store.readIndexName, query).flatMap { c =>
              if (c > 0) {
                Task.unit
              } else if (attempt >= 6) {
                Task.error(new RuntimeException(
                  s"OpenSearch commit barrier failed: doc not searchable after refresh (index=${store.readIndexName} id=$id)"
                ))
              } else {
                // Retry with a small backoff; also re-refresh (best effort).
                client.refreshIndex(store.writeIndexName)
                  .attempt
                  .unit
                  .next(Task.sleep(delay))
                  .next(loop(attempt + 1, (delay * 2).min(1.second)))
              }
            }
          }
          loop(attempt = 0, delay = 10.millis)
      }

      // IMPORTANT:
      // `wroteToIndex` is flipped inside `flushBuffer`, which runs during `super._commit`.
      // So we must decide whether to refresh AFTER `super._commit` has executed.
      val refreshAndBarrierIfWrote: Task[Unit] = Task.defer {
        // NOTE:
        // `refreshPolicy` controls per-bulk behavior (OpenSearch `refresh` query param) and defaults to false for
        // throughput. LightDB's contract is "searchable after commit", so we still force a refresh at commit
        // boundaries when we've written to the index.
        if (wroteToIndex) client.refreshIndex(store.writeIndexName).next(awaitSearchableBarrier) else Task.unit
      }

      super._commit.next(refreshAndBarrierIfWrote).attempt.flatMap {
        case Success(_) =>
          commitResult = Some(Success(()))
          Task.unit
        case Failure(t) =>
          // IMPORTANT:
          // If a bulk flush fails (e.g. mapping error), we want subsequent commits (including Store.release's implicit
          // commit) to rethrow the same error without re-flushing the same ops. Otherwise we can duplicate side-effects
          // like dead-letter captures.
          commitResult = Some(Failure(t))
          Task.error(t)
      }
  }

  override protected def _rollback: Task[Unit] = super._rollback.next(Task {
    // Allow new work after a rollback.
    commitResult = None
    wroteToIndex = false
  })

  private def cursorSafeFilterPath(base: Option[String]): Option[String] = base match {
    case None => None
    case Some(fp) =>
      // Cursor pagination requires `hits.hits.sort` to be present in the response to create the next cursor token.
      val parts = fp.split(",").toVector.map(_.trim).filter(_.nonEmpty)
      val hasSort = parts.exists(p => p == "hits.hits.sort" || p.startsWith("hits.hits.sort"))
      if (hasSort) Some(fp) else Some((parts :+ "hits.hits.sort").mkString(","))
  }

  /**
   * In OpenSearch join-domains, multiple LightDB collections share a single physical index.
   * We must scope searches and aggregations to this logical collection's join-type; otherwise
   * hits/facets can include documents from other types in the same join index.
   */
  private def joinTypeFilterDsl: Json = {
    // Support both:
    // 1) Proper OpenSearch join field mapping ("type":"join") where filtering uses `term(joinFieldName, "<type>")`
    // 2) Legacy / dynamically-mapped object join field where child docs store `{ "name": "<type>", "parent": "<id>" }`
    //    and filtering must use `term(joinFieldName.name.keyword, "<type>")` (or `.name` if no keyword subfield).
    val typeValue = str(store.name)
    val candidates = List(
      OpenSearchDsl.term(config.joinFieldName, typeValue),
      OpenSearchDsl.term(s"${config.joinFieldName}.name.keyword", typeValue),
      OpenSearchDsl.term(s"${config.joinFieldName}.name", typeValue)
    )
    OpenSearchDsl.boolQuery(should = candidates, minimumShouldMatch = Some(1))
  }

  private def applyJoinTypeFilter(body: Json): Json = {
    if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
      val joinTerm = joinTypeFilterDsl
      body match {
        case o: Obj =>
          val existingQuery = o.value.get("query").getOrElse(OpenSearchDsl.matchAll())
          val scoped = OpenSearchDsl.boolQuery(must = List(existingQuery), filter = List(joinTerm))
          obj((o.value.toSeq.filterNot(_._1 == "query") :+ ("query" -> scoped)): _*)
        case other =>
          other
      }
    } else {
      body
    }
  }

  private def scoringRequested[V](q: Query[Doc, Model, V]): Boolean = {
    // `minDocScore` flips `scoreDocs=true` in LightDB core, but for filter-only join queries we intentionally
    // keep constant scoring so `minDocScore` behaves predictably (see OpenSearchNativeExistsChildSpec).
    val wantsScores = q.scoreDocs && q.minDocScore.isEmpty
    val wantsBestMatchSort = q.sort.exists {
      case Sort.BestMatch(_) => true
      case _ => false
    }
    wantsScores || wantsBestMatchSort
  }

  private def effectiveJoinScoreMode[V](q: Query[Doc, Model, V]): String = {
    if (scoringRequested(q) && config.joinScoreMode == "none") {
      // If the query is explicitly scoring/sorting by relevance, propagate child scores so joins don't collapse to constants.
      "max"
    } else {
      config.joinScoreMode
    }
  }

  private def searchBuilderFor[V](q: Query[Doc, Model, V]): OpenSearchSearchBuilder[Doc, Model] =
    new OpenSearchSearchBuilder[Doc, Model](
      store.model,
      joinScoreMode = effectiveJoinScoreMode(q),
      keywordNormalize = config.keywordNormalize,
      allowScriptSorts = config.allowScriptSorts,
      facetAggMaxBuckets = config.facetAggMaxBuckets,
      facetChildCountMode = config.facetChildCountMode,
      facetChildCountPrecisionThreshold = config.facetChildCountPrecisionThreshold,
      facetChildCountPageSize = config.facetChildCountPageSize
    )

  /**
   * Overrides the configured refresh policy for this transaction only.
   *
   * Examples: Some("true"), Some("wait_for"), Some("false"), or None for OpenSearch default.
   */
  def withRefreshPolicy(refreshPolicy: Option[String]): this.type = {
    refreshPolicyOverride = Some(refreshPolicy)
    this
  }

  def withRefreshPolicyWaitFor(): this.type = withRefreshPolicy(Some("wait_for"))

  def withRefreshPolicyTrue(): this.type = withRefreshPolicy(Some("true"))

  def withRefreshPolicyFalse(): this.type = withRefreshPolicy(Some("false"))

  /**
   * Transaction-scoped override for OpenSearch index `refresh_interval`.
   *
   * This is useful for high-throughput rebuild/reindex workflows:
   * - set `-1` to disable refresh during bulk writes
   * - optionally restore the previous interval on close
   *
   * Note: This changes the index setting (not just per-request behavior). Use only when you control concurrency
   * against the target index.
   */
  def withRefreshInterval(interval: String, restoreOnClose: Boolean = true): this.type = {
    refreshIntervalOverride = Some(interval)
    restoreRefreshIntervalOnClose = restoreOnClose
    this
  }

  private def effectiveRefreshPolicy: Option[String] =
    refreshPolicyOverride.getOrElse(config.refreshPolicy)

  private def sequence[A](tasks: List[Task[A]]): Task[List[A]] =
    tasks.foldLeft(Task.pure(List.empty[A])) { (acc, t) =>
      acc.flatMap(list => t.map(v => list :+ v))
    }

  private def loadDoc(id: Id[Doc], hydratedSource: Json): Task[Doc] = store.storeMode match {
    case StoreMode.All() =>
      Task.pure(hydratedSource.as[Doc](store.model.rw))
    case StoreMode.Indexes(_) =>
      parent.getOrElse(this).apply(id)
  }

  private def materialize[V](conversion: Conversion[Doc, V],
                             id: Id[Doc],
                             hydratedSource: Json): Task[V] = conversion match {
    case Conversion.Value(field) =>
      Task.pure {
        val j = if (field.name == "_id") Str(id.value) else hydratedSource.asObj.get(field.name).getOrElse(Null)
        j.as(field.rw).asInstanceOf[V]
      }
    case Conversion.Doc() =>
      loadDoc(id, hydratedSource).map(_.asInstanceOf[V])
    case Conversion.Converted(c) =>
      loadDoc(id, hydratedSource).map(doc => c(doc).asInstanceOf[V])
    case Conversion.Json(fields) =>
      Task.pure {
        val o = obj(fields.map { f =>
          if (f.name == "_id") f.name -> Str(id.value) else f.name -> hydratedSource.asObj.get(f.name).getOrElse(Null)
        }: _*)
        o.asInstanceOf[V]
      }
    case Conversion.Materialized(fields) =>
      Task.pure {
        val o = obj(fields.map { f =>
          if (f.name == "_id") f.name -> Str(id.value) else f.name -> hydratedSource.asObj.get(f.name).getOrElse(Null)
        }: _*)
        lightdb.materialized.MaterializedIndex[Doc, Model](o, store.model).asInstanceOf[V]
      }
    case Conversion.DocAndIndexes() =>
      val indexes = obj(store.fields.filter(_.indexed).map { f =>
        if (f.name == "_id") f.name -> Str(id.value) else f.name -> hydratedSource.asObj.get(f.name).getOrElse(Null)
      }: _*)
      loadDoc(id, hydratedSource).map { doc =>
        lightdb.materialized.MaterializedAndDoc[Doc, Model](indexes, store.model, doc).asInstanceOf[V]
      }
    case d: Conversion.Distance[Doc @unchecked, _] =>
      val state = new IndexingState
      loadDoc(id, hydratedSource).map { doc =>
        val f = d.field.asInstanceOf[lightdb.field.Field[Doc, List[Geo]]]
        val geos = f.get(doc, f, state)
        val distances = geos.map(g => Spatial.distance(d.from, g))
        DistanceAndDoc(doc, distances).asInstanceOf[V]
      }
  }

  private def applyTrackTotalHits[V](q: Query[Doc, Model, V], body: Json): Json = {
    if (!q.countTotal) {
      body
    } else {
      config.trackTotalHitsUpTo match {
        case Some(limit) =>
          body match {
            case o: Obj =>
              obj((o.value.toSeq.filterNot(_._1 == "track_total_hits") :+ ("track_total_hits" -> num(limit))): _*)
            case other => other
          }
        case None =>
          body
      }
    }
  }

  /**
   * OpenSearch `_delete_by_query` only accepts refresh=true|false (not wait_for).
   * Normalize to keep `truncate` working when refreshPolicy=wait_for.
   */
  private def refreshForDeleteByQuery: Option[String] = effectiveRefreshPolicy match {
    case Some("wait_for") => Some("true")
    case other => other
  }

  private def validateFilters(q: Query[Doc, Model, _]): Task[Unit] = Task {
    val storeMode = store.storeMode
    if (Query.Validation || (Query.WarnFilteringWithoutIndex && storeMode.isAll)) {
      val notIndexed = q.filter.toList.flatMap(_.fields(q.model)).filter(!_.indexed)
      if (storeMode.isIndexes) {
        if (notIndexed.nonEmpty) {
          throw NonIndexedFieldException(q, notIndexed)
        }
      } else {
        if (Query.WarnFilteringWithoutIndex && notIndexed.nonEmpty) {
          scribe.warn(s"Inefficient query filtering on non-indexed field(s): ${notIndexed.map(_.name).mkString(", ")}")
        }
      }
    }
  }

  private def validateOffsetPagination(q: Query[Doc, Model, _]): Task[Unit] = Task {
    val size = q.limit.orElse(q.pageSize).getOrElse(10)
    val end = q.offset + size
    if (end > config.maxResultWindow) {
      throw new IllegalArgumentException(
        s"OpenSearch offset pagination exceeds index.max_result_window=${config.maxResultWindow} (offset=${q.offset} size=$size). " +
          s"Use cursor pagination (OpenSearch `search_after`) via `OpenSearchQuerySyntax.cursorPage`, " +
          s"or raise `lightdb.opensearch.maxResultWindow` if you intentionally need deep offsets."
      )
    }
  }

  private def prepared[V](q: Query[Doc, Model, V]): Task[Query[Doc, Model, V]] = for {
    resolved <- FilterPlanner.resolve(q.filter, q.model, resolveExistsChild = !store.supportsNativeExistsChild)
    optimizedFilter = if (q.optimize) {
      resolved.map(QueryOptimizer.optimize)
    } else {
      resolved
    }
    q2 = q.copy(filter = optimizedFilter)
    _ <- validateFilters(q2)
    _ <- validateOffsetPagination(q2)
  } yield q2

  def groupBy[G, V](query: Query[Doc, Model, V],
                    groupField: lightdb.field.Field[Doc, G],
                    docsPerGroup: Int,
                    groupOffset: Int,
                    groupLimit: Option[Int],
                    groupSort: List[lightdb.Sort],
                    withinGroupSort: List[lightdb.Sort],
                    includeScores: Boolean,
                    includeTotalGroupCount: Boolean): Task[OpenSearchGroupedSearchResults[Doc, Model, G, V]] = Task.defer {
    val limit = groupLimit.getOrElse(100_000_000)
    if (limit <= 0) Task.error(new RuntimeException(s"Group limit must be positive but was $limit"))
    else if (docsPerGroup <= 0) Task.error(new RuntimeException(s"Docs per group must be positive but was $docsPerGroup"))
    else {
      val sb = searchBuilderFor(query)
      val groupFieldName = sb.exactFieldName(groupField.asInstanceOf[lightdb.field.Field[Doc, _]])
      val q = query.filter.getOrElse(lightdb.filter.Filter.Multi[Doc](minShould = 0))
      val qDsl0 = sb.filterToDsl(q)
      val qDsl = if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
        // Scope group aggregations to this join type to avoid cross-type contamination in join-domain indices.
        OpenSearchDsl.boolQuery(
          must = List(qDsl0),
          filter = List(joinTypeFilterDsl)
        )
      } else {
        qDsl0
      }
      val withinSortDsl = sb.sortsToDsl(withinGroupSort)
      val size = groupOffset + limit

      val groupAgg = obj(
        "terms" -> obj(
          "field" -> str(groupFieldName),
          "size" -> num(size)
        ),
        "aggs" -> obj(
          "top" -> obj(
            "top_hits" -> obj(
              "size" -> num(docsPerGroup),
              "track_scores" -> bool(includeScores),
              "_source" -> bool(true),
              // If no within-group sort is provided, keep `_score` ordering.
              // Otherwise sort within the group by the provided sorts.
              "sort" -> (if (withinSortDsl.nonEmpty) arr(withinSortDsl: _*) else arr(obj("_score" -> obj("order" -> str("desc")))))
            )
          )
        )
      )

      val aggs = if (includeTotalGroupCount) {
        obj(
          "groups" -> groupAgg,
          "total_groups" -> obj("cardinality" -> obj("field" -> str(groupFieldName)))
        )
      } else {
        obj("groups" -> groupAgg)
      }

      val body = obj(
        "query" -> qDsl,
        "from" -> num(0),
        "size" -> num(0),
        "track_total_hits" -> bool(false),
        "aggregations" -> aggs
      )

      client.search(store.readIndexName, body).flatMap { json =>
        val aggsObj = json.asObj.get("aggregations").getOrElse(obj()).asObj
        val totalGroups = if (includeTotalGroupCount) {
          aggsObj
            .get("total_groups")
            .flatMap(_.asObj.get("value"))
            .map(_.asInt)
        } else None

        val buckets = aggsObj
          .get("groups")
          .flatMap(_.asObj.get("buckets"))
          .map(_.asArr.value.toList)
          .getOrElse(Nil)

        val groupTasks: List[Task[OpenSearchGroupedResult[G, V]]] =
          buckets.map(_.asObj).drop(groupOffset).take(limit).map { b =>
            val groupKey = b.get("key").getOrElse(throw new RuntimeException("Missing group key")).asString
            val topHits = b
              .get("top")
              .flatMap(_.asObj.get("hits"))
              .flatMap(_.asObj.get("hits"))
              .map(_.asArr.value.toList)
              .getOrElse(Nil)

            val resultTasks: List[Task[(V, Double)]] = topHits.map(_.asObj).map { h =>
              val id = Id[Doc](h.get("_id").getOrElse(throw new RuntimeException("OpenSearch hit missing _id")).asString)
              val score = h.get("_score") match {
                case Some(Null) | None => 0.0
                case Some(j) => j.asDouble
              }
              val source = h.get("_source").getOrElse(obj())
              val hydratedSource = stripInternalFields(sourceWithId(source, id))
              materialize(query.conversion, id, hydratedSource).map(v => (v, score))
            }

            sequence(resultTasks).map(results => OpenSearchGroupedResult(groupKey.asInstanceOf[G], results))
          }

        sequence(groupTasks).map { groups =>
          OpenSearchGroupedSearchResults(
            model = store.model,
            offset = groupOffset,
            limit = Some(limit),
            totalGroups = totalGroups,
            groups = groups,
            transaction = this
          )
        }
      }
    }
  }

  /**
   * OpenSearch forbids user-defined fields named `_id` in the document source (it's a metadata field).
   * We always write the document id as the OpenSearch document id, and strip `_id` from _source.
   */
  private def prepareForIndexing(doc: Doc): OpenSearchDocEncoding.PreparedIndex =
    OpenSearchDocEncoding.prepareForIndexing(
      doc = doc,
      storeName = store.name,
      fields = store.fields,
      config = config
    )

  private def sourceWithId(source: Json, id: Id[Doc]): Json = source match {
    case o: Obj =>
      val withId = obj((o.value.toSeq :+ ("_id" -> Str(id.value))): _*)
      normalizeJsonFields(withId)
    case _ => obj("_id" -> Str(id.value))
  }

  private def normalizeJsonFields(source: Json): Json = source match {
    case o: Obj =>
      val jsonFieldNames = store.fields
        .filter(f => f.rw.definition == fabric.define.DefType.Json)
        .map(_.name)
        .toSet
      if (jsonFieldNames.isEmpty) {
        source
      } else {
        val updated = o.value.toSeq.map {
          case (k, Str(s, _)) if jsonFieldNames.contains(k) =>
            k -> Try(JsonParser(s)).getOrElse(Str(s))
          case kv => kv
        }
        obj(updated: _*)
      }
    case _ => source
  }

  private def stripInternalFields(source: Json): Json =
    OpenSearchDocEncoding.stripInternalFields(source, config)

  private def escapeReservedIds(json: Json): Json = OpenSearchDocEncoding.escapeReservedIds(json)
  private def unescapeReservedIds(json: Json): Json = OpenSearchDocEncoding.unescapeReservedIds(json)

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = {
    val pageSize = 1000

    // Avoid recursive append chains that retain all previous pages.
    rapid.Stream(
      Task.defer {
        val lock = new AnyRef

        var searchAfter: Option[Json] = None
        var currentPull: rapid.Pull[Json] = rapid.Pull.fromList(Nil)
        var currentPullInitialized: Boolean = false
        var done: Boolean = false

        def closeCurrentPull(): Unit = {
          try currentPull.close.handleError(_ => Task.unit).sync()
          catch { case _: Throwable => () }
        }

        def fetchNextPull(): Unit = {
          closeCurrentPull()
          val query = OpenSearchDsl.prefix(OpenSearchTemplates.InternalIdField, prefix)
          val sort = arr(
            // IMPORTANT:
            // Never sort on OpenSearch metadata field `_id` here. Sorting on `_id` can trigger fielddata loading
            // and trip the OpenSearch fielddata circuit breaker on large indices.
            //
            // `__lightdb_id` is a keyword field with doc_values, safe for sorting and stable pagination.
            obj(OpenSearchTemplates.InternalIdField -> obj("order" -> str("asc")))
          )
          val body = obj(
            "query" -> query,
            "size" -> num(pageSize),
            "track_total_hits" -> bool(false),
            "sort" -> sort
          )
          val body2 = searchAfter match {
            case Some(sa) => obj((body.asObj.value.toSeq :+ ("search_after" -> sa)): _*)
            case None => body
          }

          val pageJson = client.search(store.readIndexName, body2).sync()
          val hits = pageJson.asObj.get("hits").getOrElse(obj()).asObj.get("hits").getOrElse(arr()).asArr.value.toVector
          if (hits.isEmpty) {
            done = true
            currentPull = rapid.Pull.fromList(Nil)
          } else {
            val docs = hits.map(_.asObj).map { h =>
              val id = Id[Doc](h.get("_id").getOrElse(throw new RuntimeException("OpenSearch hit missing _id")).asString)
              val source = h.get("_source").getOrElse(obj())
              stripInternalFields(sourceWithId(source, id))
            }.toList

            searchAfter = hits.last.asObj.get("sort").map(_.asArr.value.json)
            currentPull = rapid.Pull.fromList(docs)
          }
          currentPullInitialized = true
        }

        val pullTask: Task[rapid.Step[Json]] = Task {
          lock.synchronized {
            @annotation.tailrec
            def loop(): rapid.Step[Json] = {
              if (done) {
                rapid.Step.Stop
              } else {
                if (!currentPullInitialized) {
                  fetchNextPull()
                }

                currentPull.pull.sync() match {
                  case e: rapid.Step.Emit[Json] =>
                    e
                  case rapid.Step.Skip =>
                    loop()
                  case rapid.Step.Concat(inner) =>
                    currentPull = inner
                    loop()
                  case rapid.Step.Stop =>
                    if (searchAfter.isEmpty) {
                      done = true
                      closeCurrentPull()
                      rapid.Step.Stop
                    } else {
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

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.force(doSearch[Json](Query[Doc, Model, Json](this, Conversion.Json(store.fields))).map(_.stream))

  /**
   * BufferedWritingTransaction only supports `_id` reads by default. We preserve OpenSearchTransaction's richer
   * behavior by delegating non-id unique reads to OpenSearch.
   */
  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    if (index == store.idField) {
      // Use BufferedWritingTransaction logic for read-your-writes on `_id`.
      super._get(index, value)
    } else {
      // Fallback to query by unique field (assumes field is indexed).
      beforeRead.next {
        val filter = lightdb.filter.Filter.Equals[Doc, V](index.name, value)
        val q = Query[Doc, Model, Doc](this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
        doSearch[Doc](q).flatMap(_.list).map(_.headOption)
      }
    }
  }

  override protected def _count: Task[Int] = beforeRead.next {
    if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
      // In join-domains, multiple logical collections share a single index. Scope counts to this collection's join type.
      client.count(store.readIndexName, obj("query" -> joinTypeFilterDsl))
    } else {
      client.count(store.readIndexName, obj("query" -> obj("match_all" -> obj())))
    }
  }

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = {
    if (index == store.idField) {
      // Use BufferedWritingTransaction buffering semantics for deletes.
      super._delete(index, value)
    } else {
      // Resolve by querying the doc then delete by _id (buffered).
      _get(index, value).flatMap {
        case Some(doc) => super._delete(store.idField, doc._id)
        case None => Task.pure(false)
      }
    }
  }

  override protected def directGet[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    if (index == store.idField) {
      val id = value.asInstanceOf[Id[Doc]]
      client.getDoc(store.readIndexName, id.value).map(_.map { json =>
        stripInternalFields(sourceWithId(json, id)).as[Doc](store.model.rw)
      })
    } else {
      Task.error(new UnsupportedOperationException(s"OpenSearch directGet only supports _id, but ${index.name} was attempted"))
    }
  }

  /**
   * Flush buffered write ops to OpenSearch using bulk indexing.
   *
   * NOTE: This may run before `commit` if the buffered map exceeds `BufferedWritingTransaction.MaxTransactionWriteBuffer`.
   * In that case, rollback can no longer fully undo side effects. This tradeoff is intentional to avoid OOM.
   */
  override protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit] = Task.defer {
    def withIngestPermit[A](task: => Task[A]): Task[A] =
      OpenSearchIngestLimiter.withPermit(
        key = config.normalizedBaseUrl,
        maxConcurrent = config.ingestMaxConcurrentRequests
      )(task)

    def chunkOps(list: List[OpenSearchBulkOp]): List[List[OpenSearchBulkOp]] = {
      val maxDocs = config.bulkMaxDocs
      val maxBytes = config.bulkMaxBytes
      val chunks = scala.collection.mutable.ListBuffer.empty[List[OpenSearchBulkOp]]
      val current = scala.collection.mutable.ListBuffer.empty[OpenSearchBulkOp]
      var currentBytes = 0

      def estimateBytes(op: OpenSearchBulkOp): Int = op match {
        case OpenSearchBulkOpIndex(index, id, source, routing) =>
          val routingPart = routing.map(r => s""","routing":"$r"""").getOrElse("")
          val meta = s"""{"index":{"_index":"$index","_id":"$id"$routingPart}}"""
          val src = JsonFormatter.Compact(source)
          meta.length + 1 + src.length + 1
        case OpenSearchBulkOpDelete(index, id, routing) =>
          val routingPart = routing.map(r => s""","routing":"$r"""").getOrElse("")
          val meta = s"""{"delete":{"_index":"$index","_id":"$id"$routingPart}}"""
          meta.length + 1
      }

      list.foreach { op =>
        val b = estimateBytes(op)
        val wouldExceedDocs = current.nonEmpty && current.size >= maxDocs
        val wouldExceedBytes = current.nonEmpty && (currentBytes + b) > maxBytes
        if (wouldExceedDocs || wouldExceedBytes) {
          chunks += current.toList
          current.clear()
          currentBytes = 0
        }
        current += op
        currentBytes += b
      }
      if (current.nonEmpty) chunks += current.toList
      chunks.toList
    }

    applyIndexTuningIfNeeded.next(
      stream.toList.flatMap { ops =>
        val indexOps: List[OpenSearchBulkOp] = ops.flatMap {
          case WriteOp.Insert(doc) =>
            val prepared = prepareForIndexing(doc)
            Some(OpenSearchBulkOp.index(store.writeIndexName, doc._id.value, prepared.source, prepared.routing))
          case WriteOp.Upsert(doc) =>
            val prepared = prepareForIndexing(doc)
            Some(OpenSearchBulkOp.index(store.writeIndexName, doc._id.value, prepared.source, prepared.routing))
          case WriteOp.Delete(_) =>
            None
        }

        val deleteOps: List[OpenSearchBulkOp] =
          ops.collect { case WriteOp.Delete(id) => id.value }.map { id =>
            // Join-parent docs are indexed with routing = _id. Providing routing here avoids shard misroutes.
            // For non-join or non-parent stores, routing is unnecessary.
            val routing =
              if (config.joinDomain.nonEmpty && config.joinRole.contains("parent")) Some(id) else None
            OpenSearchBulkOp.delete(store.writeIndexName, id, routing = routing)
          }

        val bulkTask: Task[Unit] =
          if (indexOps.isEmpty && deleteOps.isEmpty) Task.unit
          else {
            wroteToIndex = true
            // TEMP DEBUG: record when we start pushing bulks for a store during a long reindex.
            scribe.warn(s"[reindex-trace] OpenSearch flushBuffer store=${store.name} index=${store.writeIndexName} ops=${ops.size} indexOps=${indexOps.size} deleteOps=${deleteOps.size} refresh=${effectiveRefreshPolicy.getOrElse("<default>")}")
            // Track a recent indexed document id so commit can validate searchability post-refresh.
            // (Only meaningful for index/upsert, not pure deletes.)
            indexOps.lastOption match {
              case Some(OpenSearchBulkOpIndex(_, id, _, _)) =>
                lastIndexedIdForSearchBarrier = Some(id)
              case _ => // ignore
            }
            val allOps: List[OpenSearchBulkOp] = indexOps ::: deleteOps
            val chunks = chunkOps(allOps)
            val chunkTask = (chunk: List[OpenSearchBulkOp]) => {
              // TEMP DEBUG: only log the first chunk of each flushBuffer invocation to avoid flooding logs.
              // If it hangs here, we'll see it.
              if (chunk eq chunks.head) {
                scribe.warn(s"[reindex-trace] OpenSearch bulk start store=${store.name} docs=${chunk.size} bytes~=${chunk.size} (building request)")
              }
              val body = OpenSearchBulkRequest(chunk).toBulkNdjson
              if (chunk eq chunks.head) {
                scribe.warn(s"[reindex-trace] OpenSearch bulk send store=${store.name} bytes=${body.length} docs=${chunk.size}")
              }
              if (config.metricsEnabled) {
                OpenSearchMetrics.recordBulkAttempt(config.normalizedBaseUrl, docs = chunk.size, bytes = body.length)
              }
              withIngestPermit {
                client.bulkResponse(body, refresh = effectiveRefreshPolicy)
              }.flatMap { json =>
                if (chunk eq chunks.head) {
                  val hasErrors = json.asObj.get("errors").exists(_.asBoolean)
                  scribe.warn(s"[reindex-trace] OpenSearch bulk response store=${store.name} received (errors=$hasErrors)")
                }
                val errors = json.asObj.get("errors").exists(_.asBoolean)
                if (!errors) {
                  Task.unit
                } else {
                  if (config.metricsEnabled) {
                    OpenSearchMetrics.recordFailure(config.normalizedBaseUrl)
                  }
                  // Dead letters are only meaningful for index/upsert failures; deletes are best-effort.
                  val indexChunk = chunk.collect { case i: OpenSearchBulkOpIndex => i }
                  captureBulkDeadLetters(indexChunk, json).attempt.unit.next {
                    val firstError = json.asObj
                      .get("items")
                      .map(_.asArr.value.toList)
                      .getOrElse(Nil)
                      .iterator
                      .flatMap(j => j.asObj.value.valuesIterator.flatMap(_.asObj.get("error")).toList)
                      .take(1)
                      .toList
                      .headOption
                    val msg = firstError.map(e => s" firstError=${JsonFormatter.Compact(e)}").getOrElse("")
                    Task.error(new RuntimeException(s"OpenSearch bulk request reported errors=true.$msg"))
                  }
                }
              }
            }

            if (config.bulkConcurrency <= 1 || chunks.size <= 1) {
              chunks.foldLeft(Task.unit)((acc, c) => acc.next(chunkTask(c)))
            } else {
              parTraverseNUnit(chunks, config.bulkConcurrency)(chunkTask)
            }
          }

        // Keep explicit refresh behavior consistent with the previous implementation: if refresh=true, also call _refresh.
        val maybeExtraRefresh: Task[Unit] =
          if (effectiveRefreshPolicy.contains("true")) client.refreshIndex(store.writeIndexName) else Task.unit

        bulkTask.next(maybeExtraRefresh)
      }
    )
  }

  private def truncateWholeIndex: Task[Int] = {
    // BufferedWritingTransaction.truncate clears the write buffer before calling `_truncate`.
    // We still need to reset OpenSearchTransaction-local commit memoization so subsequent ops are valid.
    commitResult = None

    def findJoinParentStoreAndConfig(): (DocumentModel[_], List[lightdb.field.Field[_, _]], OpenSearchConfig, String) = {
      // If not a join-domain, the current store is the source of truth for mappings.
      if (config.joinDomain.isEmpty) {
        (store.model, store.fields, config, store.name)
      } else {
        val joinDomain = config.joinDomain.get
        // IMPORTANT:
        // In SplitCollection setups (ex: RocksDB storage + OpenSearch searching), the OpenSearchStore instances are not
        // necessarily registered as top-level LightDB stores; only the SplitCollection is.
        //
        // Truncate needs the JOIN-PARENT store's template/mapping so we can recreate the shared join-domain index
        // with `type: join` and correct facet mappings. So, search both:
        // - direct OpenSearchStore instances in db.stores (OpenSearch-only DBs / tests)
        // - OpenSearchStore instances nested under SplitCollection.searching (SplitCollection DBs like LogicalNetwork)
        val candidateOpenSearchStores: List[OpenSearchStore[_, _]] =
          store.lightDB.stores.flatMap {
            case os: OpenSearchStore[_, _] @unchecked =>
              List(os)
            case sc: lightdb.store.split.SplitCollection[_, _, _, _] @unchecked =>
              sc.searching match {
                case os: OpenSearchStore[_, _] @unchecked => List(os)
                case _ => Nil
              }
            case _ =>
              Nil
          }

        val maybeParent = candidateOpenSearchStores.find { s =>
          val c = OpenSearchConfig.from(store.lightDB, s.name)
          c.joinDomain.contains(joinDomain) && c.joinRole.contains("parent")
        }
        val parentStore = maybeParent.getOrElse(store.asInstanceOf[OpenSearchStore[_, _]])
        val parentConfig = OpenSearchConfig.from(store.lightDB, parentStore.name)
        // Scala 2.13 struggles with the existential/path-dependent types here; cast explicitly.
        (
          parentStore.model.asInstanceOf[DocumentModel[_]],
          parentStore.fields.asInstanceOf[List[lightdb.field.Field[_, _]]],
          parentConfig,
          parentStore.name
        )
      }
    }

    val (mappingModel, mappingFieldsAny, mappingConfig, mappingStoreName) = findJoinParentStoreAndConfig()
    val mappingFields = mappingFieldsAny.asInstanceOf[List[lightdb.field.Field[Doc, _]]]
    val body = OpenSearchTemplates.indexBody(mappingModel.asInstanceOf[Model], mappingFields, mappingConfig, mappingStoreName, maxResultWindow = mappingConfig.maxResultWindow)

    val matchAll = obj("query" -> OpenSearchDsl.matchAll())

    def isAlreadyExists(t: Throwable): Boolean = {
      val msg = Option(t.getMessage).getOrElse("").toLowerCase
      msg.contains("resource_already_exists_exception") || msg.contains("already exists")
    }

    def waitForIndexDeleted(index: String,
                            maxWait: FiniteDuration = 30.minutes,
                            poll: FiniteDuration = 1.second): Task[Unit] = Task.defer {
      val startNanos = System.nanoTime()

      def loop(): Task[Unit] =
        client.indexExists(index).flatMap {
          case false => Task.unit
          case true =>
            val elapsed = (System.nanoTime() - startNanos).nanos
            if (elapsed >= maxWait) {
              Task.error(new RuntimeException(s"Timed out waiting for OpenSearch index '$index' to be deleted (waited ${maxWait.toSeconds}s)"))
            } else {
              Task.sleep(poll).next(loop())
            }
        }

      loop()
    }

    def createIndexEventually(index: String,
                              maxWait: FiniteDuration = 30.minutes,
                              poll: FiniteDuration = 1.second): Task[Unit] = Task.defer {
      val startNanos = System.nanoTime()

      def loop(): Task[Unit] =
        client.createIndex(index, body).attempt.flatMap {
          case Success(_) =>
            Task.unit
          case Failure(t) if isAlreadyExists(t) =>
            // OpenSearch index deletion is asynchronous OR we raced another creator (ex: another thread calling
            // ensureIndexReady while reindex/truncate is in-flight).
            //
            // In the "raced creator" case, createIndex will fail with already-exists even though the index is now
            // present and usable. Treat that as success once we can confirm the index exists.
            //
            // In the "cluster-state lag" case, HEAD may flap; keep retrying until the index stabilizes or we timeout.
            client.indexExists(index).flatMap {
              case true =>
                Task.unit
              case false =>
                val elapsed = (System.nanoTime() - startNanos).nanos
                if (elapsed >= maxWait) Task.error(t)
                else Task.sleep(poll).next(loop())
            }
          case Failure(t) =>
            Task.error(t)
        }

      loop()
    }

    if (config.useIndexAlias) {
      val readAlias = store.readIndexName
      val physical = s"$readAlias${config.indexAliasSuffix}"
      val writeAliasOpt = if (config.useWriteAlias) Some(s"$readAlias${config.writeAliasSuffix}") else None

      client.aliasTargets(readAlias).flatMap { targets =>
        val deletedCountTask =
          if (targets.nonEmpty) {
            // Count docs via alias before we delete physical indices.
            client.count(readAlias, matchAll).attempt.map(_.getOrElse(0))
          } else {
            client.indexExists(physical).flatMap {
              case true => client.count(physical, matchAll).attempt.map(_.getOrElse(0))
              case false => Task.pure(0)
            }
          }

        deletedCountTask.flatMap { deleted =>
          targets.map(client.deleteIndex).tasks.unit
            .next(client.deleteIndex(physical)) // best-effort in case a stale physical name exists without alias pointing
            .next(waitForIndexDeleted(physical))
            .next(createIndexEventually(physical))
            .next(client.updateAliases(obj("actions" -> arr(
              obj("add" -> obj("index" -> str(physical), "alias" -> str(readAlias)))
            ))))
            .next {
              writeAliasOpt match {
                case Some(writeAlias) =>
                  client.updateAliases(obj("actions" -> arr(
                    obj("add" -> obj("index" -> str(physical), "alias" -> str(writeAlias), "is_write_index" -> bool(true)))
                  )))
                case None => Task.unit
              }
            }
            .map(_ => deleted)
        }
      }
    } else {
      val index = store.readIndexName
      client.count(index, matchAll).flatMap { deleted =>
        client
          .deleteIndex(index)
          .next(waitForIndexDeleted(index))
          .next(createIndexEventually(index))
          .map(_ => deleted)
      }
    }
  }

  private def truncateIndexLevel: Task[Int] = {
    // Truncate semantics:
    //
    // - Non-join indices: delete/recreate the entire physical index (fast, avoids `_delete_by_query`).
    // - Join-domain indices: multiple logical LightDB collections share a single physical index.
    //   In that case, we MUST NOT delete/recreate the entire index when truncating one collection,
    //   otherwise we'd wipe documents belonging to other join types (ex: parent/child).
    //   Instead, delete only this store's join type via `_delete_by_query`.

    // BufferedWritingTransaction.truncate clears the write buffer before calling `_truncate`.
    // We still need to reset OpenSearchTransaction-local commit memoization so subsequent ops are valid.
    commitResult = None

    // Join-domain safe truncate: delete only docs for this logical store type in the shared index.
    if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
      val q = obj("query" -> joinTypeFilterDsl)
      val startNanos = System.nanoTime()
      def elapsedMs: Long = (System.nanoTime() - startNanos) / 1_000_000L

      logger
        .warn(s"[reindex-trace] OpenSearch truncate join-domain store=${store.name} index=${store.readIndexName} starting count (elapsedMs=$elapsedMs) ...")
        .next {
          client.count(store.readIndexName, q)
        }
        .flatMap { deleted =>
          logger
            .warn(s"[reindex-trace] OpenSearch truncate join-domain store=${store.name} index=${store.readIndexName} count=$deleted; starting deleteByQuery refresh=$refreshForDeleteByQuery (elapsedMs=$elapsedMs) ...")
            .next {
              client.deleteByQuery(store.readIndexName, q, refresh = refreshForDeleteByQuery)
            }
            .map { _ =>
              scribe.warn(s"[reindex-trace] OpenSearch truncate join-domain store=${store.name} index=${store.readIndexName} deleteByQuery done (elapsedMs=$elapsedMs).")
              deleted
            }
        }
    } else {
      truncateWholeIndex
    }
  }

  /**
   * Truncate all documents in the physical OpenSearch index, even for join-domain stores.
   * This is intended for full rebuilds where both parent + child docs are being reindexed.
   */
  def truncateAll: Task[Int] = Task.defer {
    var count = 0
    withWriteBuffer { wb =>
      Task {
        count = wb.map.size
        wb.copy(Map.empty)
      }
    }.flatMap { _ =>
      truncateWholeIndex.map(_ + count)
    }
  }

  // BufferedWritingTransaction requires an `_truncate` hook for its own `truncate` implementation.
  // We implement it by delegating to our index-level truncate logic.
  override protected def _truncate: Task[Int] =
    truncateIndexLevel

  private def captureBulkDeadLetters(chunk: List[OpenSearchBulkOpIndex], bulkResponse: Json): Task[Unit] = {
    if (!config.deadLetterEnabled) {
      Task.unit
    } else {
      val deadIndex = store.deadLetterIndexName
      val items = bulkResponse.asObj.get("items").map(_.asArr.value.toVector).getOrElse(Vector.empty)

      def ensureDeadIndex: Task[Unit] =
        client.indexExists(deadIndex).flatMap {
          case true => Task.unit
          case false =>
            client.createIndex(deadIndex, obj("mappings" -> obj("dynamic" -> bool(true))))
        }

      def errorsForOp(i: Int): Option[(Int, Json)] = {
        items.lift(i).flatMap { item =>
          // item looks like { "index": { "_id": "...", "status": 400, "error": {...} } }
          val actionObj = item.asObj.value.valuesIterator.toList.headOption.map(_.asObj)
          for {
            ao <- actionObj
            status <- ao.get("status").map(_.asInt)
            err <- ao.get("error")
          } yield (status, err)
        }
      }

      val failures = chunk.zipWithIndex.flatMap { case (op, i) =>
        errorsForOp(i).map { case (status, err) =>
          val doc = obj(
            "ts" -> num(System.currentTimeMillis()),
            "store" -> str(store.name),
            "target_index" -> str(store.writeIndexName),
            "op" -> str("index"),
            "id" -> str(op.id),
            "routing" -> op.routing.map(str).getOrElse(Null),
            "status" -> num(status),
            "error" -> err,
            "source" -> op.source
          )
          (Unique.sync(), doc)
        }
      }

      if (failures.isEmpty) {
        Task.unit
      } else {
        ensureDeadIndex.next {
          failures.foldLeft(Task.unit) { case (acc, (id, doc)) =>
            acc.next(client.indexDoc(deadIndex, id, doc, refresh = Some("true")))
          }
        }
      }
    }
  }

  private def parTraverseNUnit[A](values: List[A], parallelism: Int)(f: A => Task[Unit]): Task[Unit] = Task.defer {
    val sem = new Semaphore(parallelism, true)
    values.map { a =>
      Task.defer {
        sem.acquire()
        f(a).guarantee(Task(sem.release()))
      }
    }.tasksPar.unit
  }

  override protected def _close: Task[Unit] =
    // Best-effort restore of any index-level tuning we applied, then close the transaction.
    restoreIndexTuningIfNeeded.next(super._close)

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    Task.defer {
      beforeRead.next {
        prepared(query).flatMap { q =>
          val body0 = applyTrackTotalHits(q, searchBuilderFor(q).build(q))
          val body = applyJoinTypeFilter(body0)
          client.search(store.readIndexName, body).flatMap { json =>
            val hitsObj = json.asObj.get("hits").getOrElse(obj()).asObj
            val total = if (q.countTotal) {
              Some(hitsObj.get("total").getOrElse(obj()).asObj.get("value").getOrElse(num(0)).asInt)
            } else {
              None
            }
            val hitsArr = hitsObj.get("hits").getOrElse(arr()).asArr
            val hits = hitsArr.value.toVector.map(_.asObj)

            val aggs = json.asObj.get("aggregations").map(_.asObj).getOrElse(obj().asObj)

            def tokenIsChildAtPath(fq: FacetQuery[Doc], token: String): Boolean = {
              val prefix = if (fq.path.isEmpty) "" else fq.path.mkString("/") + "/"
              if (fq.path.isEmpty) {
                token == "$ROOT$" || !token.contains("/")
              } else {
                token.startsWith(prefix) && !token.drop(prefix.length).contains("/")
              }
            }

            def tokenToLabel(fq: FacetQuery[Doc], token: String): String = {
              val prefix = if (fq.path.isEmpty) "" else fq.path.mkString("/") + "/"
              if (fq.path.isEmpty) {
                token
              } else if (token.startsWith(prefix)) {
                token.drop(prefix.length)
              } else {
                token.split("/").lastOption.getOrElse(token)
              }
            }

            def readFacetValues(fq: FacetQuery[Doc]): List[FacetResultValue] = {
              val valuesAggName = s"facet_${fq.field.name}"
              val bucketsAll = aggs
                .get(valuesAggName)
                .map(_.asObj.get("buckets").getOrElse(arr()).asArr.value.toList)
                .getOrElse(Nil)
              val buckets = bucketsAll.map(_.asObj).flatMap { b =>
                for {
                  key <- b.get("key")
                  count <- b.get("doc_count")
                } yield (key.asString, count.asInt)
              }.filter { case (token, _) => tokenIsChildAtPath(fq, token) }
              buckets.map { case (token, count) =>
                FacetResultValue(tokenToLabel(fq, token), count)
              }
            }

            def computeFacetChildCount(fq: FacetQuery[Doc]): Task[Int] = {
              val countAggName = s"facet_count_${fq.field.name}"

              val mode = config.facetChildCountMode.trim.toLowerCase
              val useCardinality = mode == "cardinality" && fq.path.isEmpty && !fq.field.hierarchical

              if (useCardinality) {
                // Fast approximate distinct count via OpenSearch cardinality agg.
                val v = aggs
                  .get(countAggName)
                  .flatMap(_.asObj.get("value"))
                  .map(_.asLong)
                  .getOrElse(0L)
                Task.pure(math.min(Int.MaxValue.toLong, math.max(0L, v)).toInt)
              } else {

                def parseCountAgg(aggObj: fabric.Obj): (Int, Option[Json]) = {
                  val bucketsAll = aggObj.get("buckets").getOrElse(arr()).asArr.value.toList
                  val tokens = bucketsAll.map(_.asObj).flatMap(_.get("key").flatMap(_.asObj.get("token"))).map(_.asString)
                  val inc = tokens.count(t => tokenIsChildAtPath(fq, t))
                  val afterKey = aggObj.get("after_key")
                  (inc, afterKey)
                }

                def fetchNext(afterKey: Json): Task[(Int, Option[Json])] = {
                  val tokenField = s"${fq.field.name}__facet"
                  val composite = obj(
                    "size" -> num(config.facetChildCountPageSize),
                    "sources" -> arr(
                      obj("token" -> obj("terms" -> obj("field" -> str(tokenField))))
                    ),
                    "after" -> afterKey
                  )
                  val countAgg = obj(countAggName -> obj("composite" -> composite))
                  val bodyNext0 = obj(
                    "query" -> body0.asObj.get("query").getOrElse(OpenSearchDsl.matchAll()),
                    "from" -> num(0),
                    "size" -> num(0),
                    "track_total_hits" -> bool(false),
                    "aggregations" -> countAgg
                  )
                  val bodyNext = applyJoinTypeFilter(bodyNext0)
                  client.search(store.readIndexName, bodyNext).map { j =>
                    val a = j.asObj
                      .get("aggregations")
                      .flatMap(_.asObj.get(countAggName))
                      .map(_.asObj)
                      .getOrElse(obj().asObj)
                    parseCountAgg(a)
                  }
                }

                def loop(afterKey: Option[Json], pages: Int, acc: Int): Task[Int] = afterKey match {
                  case None => Task.pure(acc)
                  case Some(_) if pages >= config.facetChildCountMaxPages => Task.pure(acc)
                  case Some(ak) =>
                    fetchNext(ak).flatMap { case (inc, nextAfter) =>
                      loop(nextAfter, pages + 1, acc + inc)
                    }
                }

                val firstAgg = aggs.get(countAggName).map(_.asObj).getOrElse(obj().asObj)
                val (firstInc, firstAfter) = parseCountAgg(firstAgg)
                loop(firstAfter, pages = 1, acc = firstInc)
              }
            }

            val facetResultsT: Task[Map[FacetField[Doc], FacetResult]] =
              if (q.facets.nonEmpty) {
                val tasks = q.facets.map { fq =>
                  val values = readFacetValues(fq)
                    .filterNot(_.value == "$ROOT$")
                    .sortBy(v => (-v.count, v.value))
                  val totalCount = values.map(_.count).sum
                  computeFacetChildCount(fq).map { cc =>
                    fq.field -> FacetResult(values, cc, totalCount)
                  }
                }
                sequence(tasks).map(_.toMap)
              } else {
                Task.pure(Map.empty)
              }

            val stream = rapid.Stream
              .emits(hits.toList)
              .evalMap { h =>
                val id = Id[Doc](h.get("_id").getOrElse(throw new RuntimeException("OpenSearch hit missing _id")).asString)
                val score = h.get("_score") match {
                  case Some(Null) | None => 0.0
                  case Some(j) => j.asDouble
                }
                val source = h.get("_source").getOrElse(obj())
                val hydratedSource = stripInternalFields(sourceWithId(source, id))
                materialize(q.conversion, id, hydratedSource).map(v => (v, score))
              }

            facetResultsT.map { facetResults =>
              SearchResults(
                model = store.model,
                offset = q.offset,
                limit = q.limit,
                total = total,
                streamWithScore = stream,
                facetResults = facetResults,
                transaction = this
              )
            }
          }
        }
      }
    }

  /**
   * Prefer keyset (cursor) pagination for streaming on OpenSearch when possible to avoid `max_result_window` limits.
   *
   * We only enable this when `offset == 0` (cursor pagination cannot jump to arbitrary offsets efficiently).
   * Otherwise we fall back to the default offset-based implementation (which will enforce max_result_window).
   */
  override def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] = {
    // `pageSize=None` means "don't cap results to one page". On OpenSearch, omitting `size` defaults to 10 hits,
    // so streaming can appear to "instantly complete" after a tiny first page. Normalize `None` to a sane default
    // page size so we can use `search_after` streaming.
    val normalizedQuery =
      if (query.pageSize.isEmpty) query.copy(pageSize = Some(1000)) else query

    if (normalizedQuery.pageSize.nonEmpty && normalizedQuery.offset == 0) {
      // IMPORTANT:
      // Do NOT implement cursor streaming via recursive `Stream.append(...)`. In Rapid, `Step.Concat` pushes
      // the previous pull onto a stack until the entire stream completes, which retains every prior page's
      // `hits` / `_source` JSON and will OOM on large scans.
      //
      // Implement streaming as a single Pull that fetches/drains/closes pages sequentially.
      val basePageSize = normalizedQuery.pageSize.getOrElse(1000)

      rapid.Stream(
        Task.defer {
          val lock = new AnyRef

          var cursorToken: Option[String] = None
          var remaining: Option[Int] = normalizedQuery.limit
          var currentPull: rapid.Pull[(V, Double)] = rapid.Pull.fromList(Nil)
          var currentPullInitialized: Boolean = false
          var done: Boolean = false

          def closeCurrentPull(): Unit = {
            try currentPull.close.handleError(_ => Task.unit).sync()
            catch { case _: Throwable => () }
          }

          def computeReqSize(): Int = remaining match {
            case Some(r) if r <= 0 => 0
            case Some(r) => math.min(r, basePageSize)
            case None => basePageSize
          }

          def fetchNextPull(reqSize: Int): Unit = {
            // Close the previous page pull before replacing it to release references to the prior page.
            closeCurrentPull()

            if (reqSize <= 0) {
              done = true
              currentPull = rapid.Pull.fromList(Nil)
            } else {
              val searchAfter = cursorToken.flatMap(OpenSearchCursor.decode)
              val page = doSearchAfter(query = normalizedQuery, searchAfter = searchAfter, pageSize = reqSize).sync()
              cursorToken = page.nextCursorToken
              remaining = remaining.map(_ - reqSize)
              currentPull = rapid.Stream.task(page.results.streamWithScore).sync()
            }
            currentPullInitialized = true
          }

          val pullTask: Task[rapid.Step[(V, Double)]] = Task {
            lock.synchronized {
              @annotation.tailrec
              def loop(): rapid.Step[(V, Double)] = {
                if (done) {
                  rapid.Step.Stop
                } else {
                  if (!currentPullInitialized) {
                    fetchNextPull(computeReqSize())
                  }

                  currentPull.pull.sync() match {
                    case e: rapid.Step.Emit[(V, Double)] =>
                      e
                    case rapid.Step.Skip =>
                      loop()
                    case rapid.Step.Concat(inner) =>
                      currentPull = inner
                      loop()
                    case rapid.Step.Stop =>
                      cursorToken match {
                        case Some(_) if remaining.forall(_ > 0) =>
                          currentPullInitialized = false
                          loop()
                        case _ =>
                          done = true
                          closeCurrentPull()
                          rapid.Step.Stop
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
    } else {
      super.streamScored(normalizedQuery)
    }
  }

  def doSearchAfter[V](query: Query[Doc, Model, V],
                       searchAfter: Option[Json],
                       pageSize: Int): Task[OpenSearchCursorPage[Doc, Model, V]] = beforeRead.next {
    prepared(query).flatMap { q0 =>
      val q = q0.copy(offset = 0, limit = Some(pageSize), pageSize = None, countTotal = false)
      val filter = q.filter.getOrElse(lightdb.filter.Filter.Multi[Doc](minShould = 0))

      val baseSort = if (q.sort.nonEmpty) q.sort else List(Sort.IndexOrder)
      val hasIdSort = baseSort.exists {
        case Sort.IndexOrder => true
        case _ => false
      }
      val stableSort = if (hasIdSort) baseSort else baseSort ::: List(Sort.IndexOrder)

      val baseBody0 = applyTrackTotalHits(q, searchBuilderFor(q).build(q.copy(sort = stableSort)))
      val baseBody = applyJoinTypeFilter(baseBody0)
      val body = searchAfter match {
        case Some(sa) =>
          baseBody match {
            case o: Obj => obj((o.value.toSeq :+ ("search_after" -> sa)): _*)
            case other => other
          }
        case None => baseBody
      }

      val filterPathOverride = cursorSafeFilterPath(config.searchFilterPath)
      client.search(store.readIndexName, body, filterPathOverride = filterPathOverride).map { json =>
        val hitsObj = json.asObj.get("hits").getOrElse(obj()).asObj
        val hitsArr = hitsObj.get("hits").getOrElse(arr()).asArr
        val hits = hitsArr.value.map(_.asObj)

        val total = None

        val facetResults: Map[FacetField[Doc], FacetResult] = if (q.facets.nonEmpty) {
          val aggs = json.asObj.get("aggregations").map(_.asObj).getOrElse(obj().asObj)
          q.facets.map { fq =>
            val aggName = s"facet_${fq.field.name}"
            val bucketsAll = aggs
              .get(aggName)
              .map(_.asObj.get("buckets").getOrElse(arr()).asArr.value.toList)
              .getOrElse(Nil)
            val prefix = if (fq.path.isEmpty) "" else fq.path.mkString("/") + "/"
            val buckets = bucketsAll.map(_.asObj).flatMap { b =>
              for {
                key <- b.get("key")
                count <- b.get("doc_count")
              } yield (key.asString, count.asInt)
            }.filter {
              case (token, _) if fq.path.isEmpty =>
                token == "$ROOT$" || !token.contains("/")
              case (token, _) =>
                token.startsWith(prefix) && !token.drop(prefix.length).contains("/")
            }
            val values = buckets.map {
              case (token, count) =>
                val label = if (fq.path.isEmpty) {
                  token
                } else if (token.startsWith(prefix)) {
                  token.drop(prefix.length)
                } else {
                  token.split("/").lastOption.getOrElse(token)
                }
                FacetResultValue(label, count)
            }
            // `childCount` must represent total distinct children, not just the returned top-N.
            val childCount = buckets.length
            val updatedValuesAll = values
              .filterNot(_.value == "$ROOT$")
              .sortBy(v => (-v.count, v.value))
            val updatedValues = fq.childrenLimit match {
              case Some(l) => updatedValuesAll.take(l)
              case None => updatedValuesAll
            }
            val totalCount = updatedValues.map(_.count).sum
            fq.field -> FacetResult(updatedValues, childCount, totalCount)
          }.toMap
        } else {
          Map.empty
        }

        val stream = rapid.Stream
          .emits(hits.toList)
          .evalMap { h =>
            val id = Id[Doc](h.get("_id").getOrElse(throw new RuntimeException("OpenSearch hit missing _id")).asString)
            val score = h.get("_score") match {
              case Some(Null) | None => 0.0
              case Some(j) => j.asDouble
            }
            val source = h.get("_source").getOrElse(obj())
            val hydratedSource = stripInternalFields(sourceWithId(source, id))
            materialize(q.conversion, id, hydratedSource).map(v => (v, score))
          }
        val results = SearchResults(
          model = store.model,
          offset = 0,
          limit = Some(pageSize),
          total = total,
          streamWithScore = stream,
          facetResults = facetResults,
          transaction = this
        )

        val nextCursorToken = hits.lastOption.flatMap(_.get("sort")) match {
          case Some(sortValues) if hits.length >= pageSize => Some(OpenSearchCursor.encode(sortValues))
          case _ => None
        }
        OpenSearchCursorPage(results, nextCursorToken)
      }
    }
  }

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    aggregate(query).count

  override def distinct[F](query: Query[Doc, Model, _],
                           field: lightdb.field.Field[Doc, F],
                           pageSize: Int): rapid.Stream[F] = {
    if (pageSize <= 0) {
      rapid.Stream.empty
    } else {
      rapid.Stream.force {
        beforeRead.next {
          prepared(query).flatMap { q0 =>
            val sb = searchBuilderFor(q0)
            val fieldName = sb.exactFieldName(field.asInstanceOf[lightdb.field.Field[Doc, _]])
            val baseFilter = q0.filter.getOrElse(lightdb.filter.Filter.Multi[Doc](minShould = 0))
            val qDsl0 = sb.filterToDsl(baseFilter)
            val qDsl = if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
              // Scope composite aggregation to this join type to avoid cross-type contamination.
              lightdb.opensearch.query.OpenSearchDsl.boolQuery(
                must = List(qDsl0),
                filter = List(joinTypeFilterDsl)
              )
            } else {
              qDsl0
            }

            def loop(after: Option[Json]): rapid.Stream[F] =
              rapid.Stream.force {
                val composite = {
                  val base = obj(
                    "size" -> num(pageSize),
                    "sources" -> arr(
                      obj(
                        "v" -> obj(
                          "terms" -> obj("field" -> str(fieldName))
                        )
                      )
                    )
                  )
                  after match {
                    case Some(a) =>
                      base match {
                        case o: Obj => obj((o.value.toSeq :+ ("after" -> a)): _*)
                        case other => other
                      }
                    case None => base
                  }
                }

                val body = obj(
                  "size" -> num(0),
                  "query" -> qDsl,
                  "aggs" -> obj(
                    "distinct" -> obj(
                      "composite" -> composite
                    )
                  )
                )

                client.search(store.readIndexName, body).map { json =>
                  val aggs = json.asObj.get("aggregations").map(_.asObj).getOrElse(obj().asObj)
                  val distinct = aggs.get("distinct").map(_.asObj).getOrElse(obj().asObj)
                  val buckets = distinct.get("buckets").map(_.asArr.value.toVector).getOrElse(Vector.empty)
                  val afterKey = distinct.get("after_key")

                  val values: Vector[F] = buckets.flatMap { b =>
                    val o = b.asObj
                    val keyObj = o.get("key").map(_.asObj)
                    keyObj.flatMap(_.get("v")).map { j =>
                      j.as(field.rw)
                    }
                  }

                  val current = rapid.Stream.emits(values)
                  afterKey match {
                    case Some(nextAfter) if buckets.nonEmpty =>
                      current.append(loop(Some(nextAfter)))
                    case _ =>
                      current
                  }
                }
              }

            Task.pure(loop(after = None))
          }
        }
      }
    }
  }
}

private[opensearch] sealed trait OpenSearchBulkOp

private[opensearch] object OpenSearchBulkOp {
  def index(index: String, id: String, source: Json, routing: Option[String] = None): OpenSearchBulkOp =
    OpenSearchBulkOpIndex(index, id, source, routing)

  def delete(index: String, id: String, routing: Option[String] = None): OpenSearchBulkOp =
    OpenSearchBulkOpDelete(index, id, routing)
}

private[opensearch] case class OpenSearchBulkOpIndex(index: String, id: String, source: Json, routing: Option[String]) extends OpenSearchBulkOp

private[opensearch] case class OpenSearchBulkOpDelete(index: String, id: String, routing: Option[String]) extends OpenSearchBulkOp

private[opensearch] case class OpenSearchBulkRequest(ops: List[OpenSearchBulkOp]) {
  def toBulkNdjson: String = ops.map {
    case OpenSearchBulkOpIndex(index, id, source, routing) =>
      val routingPart = routing.map(r => s""","routing":"${escapeJson(r)}"""").getOrElse("")
      val meta = s"""{"index":{"_index":"$index","_id":"${escapeJson(id)}"$routingPart}}"""
      val src = JsonFormatter.Compact(source)
      s"$meta\n$src\n"
    case OpenSearchBulkOpDelete(index, id, routing) =>
      val routingPart = routing.map(r => s""","routing":"${escapeJson(r)}"""").getOrElse("")
      val meta = s"""{"delete":{"_index":"$index","_id":"${escapeJson(id)}"$routingPart}}"""
      s"$meta\n"
  }.mkString

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")
}



