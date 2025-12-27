package lightdb.opensearch

import fabric._
import fabric.io.JsonFormatter
import fabric.io.JsonParser
import fabric.rw._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.FacetValue
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
import lightdb.store.{Conversion, StoreMode}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.{Query, SearchResults, Sort}
import lightdb.util.Aggregator
import rapid.Task
import rapid.taskSeq2Ops

import java.util.concurrent.Semaphore
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import lightdb.opensearch.OpenSearchMetrics

case class OpenSearchTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: OpenSearchStore[Doc, Model],
                                                                                    config: OpenSearchConfig,
                                                                                    client: OpenSearchClient,
                                                                                    parent: Option[Transaction[Doc, Model]]) extends CollectionTransaction[Doc, Model] with PrefixScanningTransaction[Doc, Model] {
  private val bufferedOps = mutable.ListBuffer.empty[OpenSearchBulkOp]
  @volatile private var commitResult: Option[Try[Unit]] = None
  @volatile private var refreshPolicyOverride: Option[Option[String]] = None

  private def cursorSafeFilterPath(base: Option[String]): Option[String] = base match {
    case None => None
    case Some(fp) =>
      // Cursor pagination requires `hits.hits.sort` to be present in the response to create the next cursor token.
      val parts = fp.split(",").toVector.map(_.trim).filter(_.nonEmpty)
      val hasSort = parts.exists(p => p == "hits.hits.sort" || p.startsWith("hits.hits.sort"))
      if (hasSort) Some(fp) else Some((parts :+ "hits.hits.sort").mkString(","))
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
      keywordNormalize = config.keywordNormalize
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
      val qDsl = sb.filterToDsl(q)
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

    def loop(searchAfter: Option[Json]): Task[rapid.Stream[Json]] = Task.defer {
      val query = OpenSearchDsl.prefix(OpenSearchTemplates.InternalIdField, prefix)
      val sort = arr(
        obj(OpenSearchTemplates.InternalIdField -> obj("order" -> str("asc"))),
        obj("_id" -> obj("order" -> str("asc")))
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

      client.search(store.readIndexName, body2).map { pageJson =>
        val hits = pageJson.asObj.get("hits").getOrElse(obj()).asObj.get("hits").getOrElse(arr()).asArr.value.toVector
        if (hits.isEmpty) {
          rapid.Stream.empty
        } else {
          val docs = hits.map(_.asObj).map { h =>
            val id = Id[Doc](h.get("_id").getOrElse(throw new RuntimeException("OpenSearch hit missing _id")).asString)
            val source = h.get("_source").getOrElse(obj())
            stripInternalFields(sourceWithId(source, id))
          }.toList

          val lastSort = hits.last.asObj.get("sort").map(_.asArr.value.json)
          val next = rapid.Stream.force(loop(lastSort).map(identity))
          rapid.Stream.emits(docs).append(next)
        }
      }
    }

    rapid.Stream.force(loop(None).map(identity))
  }

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.force(doSearch[Json](Query[Doc, Model, Json](this, Conversion.Json(store.fields))).map(_.stream))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    if (index == store.idField) {
      val id = value.asInstanceOf[Id[Doc]]
      client.getDoc(store.readIndexName, id.value).map(_.map { json =>
        stripInternalFields(sourceWithId(json, id)).as[Doc](store.model.rw)
      })
    } else {
      // fallback to query by unique field (assumes field is indexed)
      val filter = lightdb.filter.Filter.Equals[Doc, V](index.name, value)
      val q = Query[Doc, Model, Doc](this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
      doSearch[Doc](q).flatMap(_.list).map(_.headOption)
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    val prepared = prepareForIndexing(doc)
    bufferedOps += OpenSearchBulkOp.index(store.writeIndexName, doc._id.value, prepared.source, routing = prepared.routing)
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val prepared = prepareForIndexing(doc)
    bufferedOps += OpenSearchBulkOp.index(store.writeIndexName, doc._id.value, prepared.source, routing = prepared.routing)
    doc
  }

  override protected def _exists(id: lightdb.id.Id[Doc]): Task[Boolean] =
    client.getDoc(store.readIndexName, id.value).map(_.nonEmpty)

  override protected def _count: Task[Int] =
    if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
      // In join-domains, multiple logical collections share a single index. Scope counts to this collection's join type.
      client.count(store.readIndexName, obj("query" -> OpenSearchDsl.term(config.joinFieldName, str(store.name))))
    } else {
      client.count(store.readIndexName, obj("query" -> obj("match_all" -> obj())))
    }

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = {
    if (index == store.idField) {
      client.deleteDoc(store.writeIndexName, value.asInstanceOf[Id[Doc]].value, refresh = effectiveRefreshPolicy)
    } else {
      // resolve by querying the doc then deleting by _id
      _get(index, value).flatMap {
        case Some(doc) => client.deleteDoc(store.writeIndexName, doc._id.value, refresh = effectiveRefreshPolicy)
        case None => Task.pure(false)
      }
    }
  }

  override protected def _commit: Task[Unit] = commitResult match {
    case Some(Success(_)) => Task.unit
    case Some(Failure(t)) => Task.error(t)
    case None =>
      commitOnce.attempt.flatMap {
        case Success(_) =>
          commitResult = Some(Success(()))
          Task.unit
        case Failure(t) =>
          commitResult = Some(Failure(t))
          Task.error(t)
      }
  }

  private def commitOnce: Task[Unit] = {
    val ops = bufferedOps.toList
    if (ops.isEmpty) {
      Task.unit
    } else {
      def withIngestPermit[A](task: => Task[A]): Task[A] =
        OpenSearchIngestLimiter.withPermit(
          key = config.normalizedBaseUrl,
          maxConcurrent = config.ingestMaxConcurrentRequests
        )(task)

      def chunkOps(list: List[OpenSearchBulkOpIndex]): List[List[OpenSearchBulkOpIndex]] = {
        val maxDocs = config.bulkMaxDocs
        val maxBytes = config.bulkMaxBytes
        val chunks = scala.collection.mutable.ListBuffer.empty[List[OpenSearchBulkOpIndex]]
        val current = scala.collection.mutable.ListBuffer.empty[OpenSearchBulkOpIndex]
        var currentBytes = 0

        def estimateBytes(op: OpenSearchBulkOpIndex): Int = {
          val routingPart = op.routing.map(r => s""","routing":"$r"""").getOrElse("")
          val meta = s"""{"index":{"_index":"${op.index}","_id":"${op.id}"$routingPart}}"""
          val src = JsonFormatter.Compact(op.source)
          meta.length + 1 + src.length + 1
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

      val task = ops match {
        case (single: OpenSearchBulkOpIndex) :: Nil =>
          withIngestPermit {
            client.indexDoc(single.index, single.id, single.source, refresh = effectiveRefreshPolicy, routing = single.routing)
          }
        case many =>
          val indices = many.collect { case i: OpenSearchBulkOpIndex => i }
          val chunks = chunkOps(indices)
          val bulkTask =
            if (config.bulkConcurrency <= 1 || chunks.size <= 1) {
              chunks.foldLeft(Task.unit) { (acc, chunk) =>
                acc.next {
                  val body = OpenSearchBulkRequest(chunk).toBulkNdjson
                  if (config.metricsEnabled) {
                    OpenSearchMetrics.recordBulkAttempt(config.normalizedBaseUrl, docs = chunk.size, bytes = body.length)
                  }
                  withIngestPermit {
                    client.bulkResponse(body, refresh = effectiveRefreshPolicy)
                  }.flatMap { json =>
                    val errors = json.asObj.get("errors").exists(_.asBoolean)
                    if (!errors) {
                      Task.unit
                    } else {
                      if (config.metricsEnabled) {
                        OpenSearchMetrics.recordFailure(config.normalizedBaseUrl)
                      }
                      captureBulkDeadLetters(chunk, json).attempt.unit.next {
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
              }
            } else {
              parTraverseNUnit(chunks, config.bulkConcurrency) { chunk =>
                val body = OpenSearchBulkRequest(chunk).toBulkNdjson
                if (config.metricsEnabled) {
                  OpenSearchMetrics.recordBulkAttempt(config.normalizedBaseUrl, docs = chunk.size, bytes = body.length)
                }
                withIngestPermit {
                  client.bulkResponse(body, refresh = effectiveRefreshPolicy)
                }.flatMap { json =>
                  val errors = json.asObj.get("errors").exists(_.asBoolean)
                  if (!errors) {
                    Task.unit
                  } else {
                    if (config.metricsEnabled) {
                      OpenSearchMetrics.recordFailure(config.normalizedBaseUrl)
                    }
                    captureBulkDeadLetters(chunk, json).attempt.unit.next {
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
            }

          bulkTask.flatTap { _ =>
            if (effectiveRefreshPolicy.contains("true")) client.refreshIndex(store.writeIndexName) else Task.unit
          }
      }
      task.map(_ => bufferedOps.clear())
    }
  }

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
          (UUID.randomUUID().toString.replace("-", ""), doc)
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

  override protected def _rollback: Task[Unit] = Task {
    bufferedOps.clear()
  }

  override protected def _close: Task[Unit] = Task.unit

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    Task.defer {
      prepared(query).flatMap { q =>
        val body = applyTrackTotalHits(q, searchBuilderFor(q).build(q))
        client.search(store.readIndexName, body).map { json =>
        val hitsObj = json.asObj.get("hits").getOrElse(obj()).asObj
        val total = if (q.countTotal) {
          Some(hitsObj.get("total").getOrElse(obj()).asObj.get("value").getOrElse(num(0)).asInt)
        } else {
          None
        }
        val hitsArr = hitsObj.get("hits").getOrElse(arr()).asArr
        val hits = hitsArr.value.toVector.map(_.asObj)

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
            val childCount = buckets.length
            val updatedValues = values
              .filterNot(_.value == "$ROOT$")
              .sortBy(v => (-v.count, v.value))
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

  def doSearchAfter[V](query: Query[Doc, Model, V],
                       searchAfter: Option[Json],
                       pageSize: Int): Task[OpenSearchCursorPage[Doc, Model, V]] = prepared(query).flatMap { q0 =>
    val q = q0.copy(offset = 0, limit = Some(pageSize), pageSize = None, countTotal = false)
    val filter = q.filter.getOrElse(lightdb.filter.Filter.Multi[Doc](minShould = 0))

    val baseSort = if (q.sort.nonEmpty) q.sort else List(Sort.IndexOrder)
    val hasIdSort = baseSort.exists {
      case Sort.IndexOrder => true
      case _ => false
    }
    val stableSort = if (hasIdSort) baseSort else baseSort ::: List(Sort.IndexOrder)

    val baseBody = applyTrackTotalHits(q, searchBuilderFor(q).build(q.copy(sort = stableSort)))
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
      val hits = hitsArr.value.toVector.map(_.asObj)

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
          val childCount = buckets.length
          val updatedValues = values
            .filterNot(_.value == "$ROOT$")
            .sortBy(v => (-v.count, v.value))
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

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    aggregate(query).count

  override def doUpdate[V](query: Query[Doc, Model, V], updates: List[FieldAndValue[Doc, _]]): Task[Int] =
    super.doUpdate(query, updates)

  override def doDelete[V](query: Query[Doc, Model, V]): Task[Int] =
    super.doDelete(query)

  override def truncate: Task[Int] = {
    // In a join-domain, multiple logical LightDB collections share a single OpenSearch index.
    // Truncate must therefore be scoped to this collection's documents; otherwise a join-child truncate would wipe parents.
    val query =
      if (config.joinDomain.nonEmpty && config.joinRole.nonEmpty) {
        OpenSearchDsl.term(config.joinFieldName, str(store.name))
      } else {
        OpenSearchDsl.matchAll()
      }
    client.deleteByQuery(store.writeIndexName, obj("query" -> query), refresh = refreshForDeleteByQuery)
  }
}

private[opensearch] sealed trait OpenSearchBulkOp

private[opensearch] object OpenSearchBulkOp {
  def index(index: String, id: String, source: Json, routing: Option[String] = None): OpenSearchBulkOp =
    OpenSearchBulkOpIndex(index, id, source, routing)
}

private[opensearch] case class OpenSearchBulkOpIndex(index: String, id: String, source: Json, routing: Option[String]) extends OpenSearchBulkOp

private[opensearch] case class OpenSearchBulkRequest(ops: List[OpenSearchBulkOp]) {
  def toBulkNdjson: String = ops.map {
    case OpenSearchBulkOpIndex(index, id, source, routing) =>
      val routingPart = routing.map(r => s""","routing":"${escapeJson(r)}"""").getOrElse("")
      val meta = s"""{"index":{"_index":"$index","_id":"${escapeJson(id)}"$routingPart}}"""
      val src = JsonFormatter.Compact(source)
      s"$meta\n$src\n"
  }.mkString

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")
}



