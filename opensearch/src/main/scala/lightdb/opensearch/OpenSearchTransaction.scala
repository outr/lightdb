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

import scala.collection.mutable
import scala.util.Try

case class OpenSearchTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: OpenSearchStore[Doc, Model],
                                                                                    config: OpenSearchConfig,
                                                                                    client: OpenSearchClient,
                                                                                    parent: Option[Transaction[Doc, Model]]) extends CollectionTransaction[Doc, Model] with PrefixScanningTransaction[Doc, Model] {
  private val bufferedOps = mutable.ListBuffer.empty[OpenSearchBulkOp]
  private lazy val searchBuilder = new OpenSearchSearchBuilder[Doc, Model](store.model, joinScoreMode = config.joinScoreMode)

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
  private def refreshForDeleteByQuery: Option[String] = config.refreshPolicy match {
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
      val groupFieldName = searchBuilder.exactFieldName(groupField.asInstanceOf[lightdb.field.Field[Doc, _]])
      val q = query.filter.getOrElse(lightdb.filter.Filter.Multi[Doc](minShould = 0))
      val qDsl = searchBuilder.filterToDsl(q)
      val withinSortDsl = searchBuilder.sortsToDsl(withinGroupSort)
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
  private case class PreparedIndex(source: Json, routing: Option[String])

  private def prepareForIndexing(doc: Doc): PreparedIndex = {
    val state = new IndexingState
    var joinParentId: Option[String] = None
    val pairs = store.fields.iterator
      .filterNot(_.name == "_id")
      .map { f =>
        val j = f.getJson(doc, state)
        val normalized = escapeReservedIds(j)
        val value = normalized match {
          case _ if f.rw.definition == fabric.define.DefType.Json =>
            // Persist Json as a compact string to avoid mapping conflicts (KeyValue backing store, etc.)
            Str(JsonFormatter.Compact(normalized))
          case other =>
            other
        }
        if (config.joinDomain.nonEmpty && config.joinRole.contains("child") && config.joinParentField.contains(f.name)) {
          joinParentId = value match {
            case Str(s, _) => Some(s)
            case other => Some(other.toString)
          }
        }
        f.name -> value
      }
      .toList
    val base = obj(((OpenSearchTemplates.InternalIdField, Str(doc._id.value)) :: pairs): _*)
    val withJoin = if (config.joinDomain.nonEmpty) {
      config.joinRole match {
        case Some("parent") =>
          // join field value: { "name": "<parentType>" }
          val join = obj(
            "name" -> str(store.name)
          )
          obj((base.asObj.value.toSeq :+ (config.joinFieldName -> join)): _*)
        case Some("child") =>
          val parentId = joinParentId.getOrElse(throw new RuntimeException(
            s"Missing joinParentField='${config.joinParentField.getOrElse("")}' value for child document in joinDomain='${config.joinDomain.getOrElse("")}'"
          ))
          val join = obj(
            "name" -> str(store.name),
            "parent" -> str(parentId)
          )
          obj((base.asObj.value.toSeq :+ (config.joinFieldName -> join)): _*)
        case _ =>
          base
      }
    } else {
      base
    }
    val source = augmentFacetTokens(augmentSpatialCenters(withJoin), doc, state)

    val routing = if (config.joinDomain.nonEmpty) {
      config.joinRole match {
        case Some("parent") => Some(doc._id.value)
        case Some("child") => joinParentId
        case _ => None
      }
    } else {
      None
    }
    PreparedIndex(source = source, routing = routing)
  }

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

  private def stripInternalFields(source: Json): Json = source match {
    case o: Obj =>
      val cleaned = obj(o.value.toSeq.filterNot {
        case (k, _) =>
          k == OpenSearchTemplates.InternalIdField ||
            k == config.joinFieldName ||
            k.endsWith(OpenSearchTemplates.SpatialCenterSuffix)
      }: _*)
      unescapeReservedIds(cleaned)
    case other => unescapeReservedIds(other)
  }

  /**
   * OpenSearch reserves/handles `_id` specially. We do not store `_id` at the root (it's metadata),
   * but we *do* need to preserve nested `_id` fields (e.g. edge documents embedded inside reverse-edge docs).
   *
   * To keep round-trip materialization working, we escape nested `_id` keys to `InternalIdField` on write,
   * and unescape them back to `_id` on read (after removing the root internal id field).
   */
  private def escapeReservedIds(json: Json): Json = json match {
    case o: Obj =>
      val updated = o.value.toSeq.map { case (k, v) =>
        val key = if (k == "_id") OpenSearchTemplates.InternalIdField else k
        key -> escapeReservedIds(v)
      }
      obj(updated: _*)
    case Arr(values, _) =>
      arr(values.map(escapeReservedIds): _*)
    case other => other
  }

  private def unescapeReservedIds(json: Json): Json = json match {
    case o: Obj =>
      val updated = o.value.toSeq.map { case (k, v) =>
        val key = if (k == OpenSearchTemplates.InternalIdField) "_id" else k
        key -> unescapeReservedIds(v)
      }
      obj(updated: _*)
    case Arr(values, _) =>
      arr(values.map(unescapeReservedIds): _*)
    case other => other
  }

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

  private def geoPoint(lat: Double, lon: Double): Json =
    obj("lat" -> num(lat), "lon" -> num(lon))

  private def augmentSpatialCenters(source: Json): Json = source match {
    case o: Obj =>
      val extras = store.fields.collect {
        case f if f.isSpatial && f.name != "_id" =>
          val fieldName = f.name
          val centerFieldName = s"$fieldName${OpenSearchTemplates.SpatialCenterSuffix}"
          val geos: List[Geo] = o.value.get(fieldName) match {
            case None => Nil
            case Some(Null) => Nil
            case Some(Arr(values, _)) =>
              values.toList.flatMap(j => Try(j.as[Geo]).toOption)
            case Some(j) =>
              Try(j.as[Geo]).toOption.toList
          }
          val centers: List[Json] = if (geos.isEmpty) {
            // Match Lucene behavior: index a dummy point so docvalues exist, and exclude it at query-time.
            List(geoPoint(0.0, 0.0))
          } else {
            geos.map(g => geoPoint(g.center.latitude, g.center.longitude))
          }
          (centerFieldName, arr(centers: _*))
      }
      obj((o.value.toSeq ++ extras): _*)
    case other => other
  }

  private val RootMarker: String = "$ROOT$"

  private def augmentFacetTokens(source: Json, doc: Doc, state: IndexingState): Json = {
    val facetFields: List[FacetField[Doc]] = store.fields.collect {
      case ff: lightdb.field.Field.FacetField[_] =>
        ff.asInstanceOf[FacetField[Doc]]
    }
    if (facetFields.isEmpty) {
      source
    } else {
      val extras = facetFields.map { ff =>
        val values: List[FacetValue] = ff.get(doc, ff, state)
        val tokens: List[String] = values.flatMap { fv =>
          val p = fv.path
          if (ff.hierarchical) {
            if (p.isEmpty) {
              List(RootMarker)
            } else {
              val prefixes = p.indices.map(i => p.take(i + 1).mkString("/")).toList
              val terminal = s"${p.mkString("/")}/$RootMarker"
              prefixes ::: List(terminal)
            }
          } else {
            // Non-hierarchical facets are single-level (we only need the value token).
            if (p.isEmpty) Nil else List(p.mkString("/"))
          }
        }.distinct
        s"${ff.name}__facet" -> arr(tokens.map(str): _*)
      }
      source match {
        case o: Obj =>
          obj((o.value.toSeq ++ extras): _*)
        case _ => source
      }
    }
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
    client.count(store.readIndexName, obj("query" -> obj("match_all" -> obj())))

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = {
    if (index == store.idField) {
      client.deleteDoc(store.writeIndexName, value.asInstanceOf[Id[Doc]].value, refresh = config.refreshPolicy)
    } else {
      // resolve by querying the doc then deleting by _id
      _get(index, value).flatMap {
        case Some(doc) => client.deleteDoc(store.writeIndexName, doc._id.value, refresh = config.refreshPolicy)
        case None => Task.pure(false)
      }
    }
  }

  override protected def _commit: Task[Unit] = {
    val ops = bufferedOps.toList
    if (ops.isEmpty) {
      Task.unit
    } else {
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
          client.indexDoc(single.index, single.id, single.source, refresh = config.refreshPolicy, routing = single.routing)
        case many =>
          val indices = many.collect { case i: OpenSearchBulkOpIndex => i }
          val chunks = chunkOps(indices)
          chunks.foldLeft(Task.unit) { (acc, chunk) =>
            acc.next {
              val body = OpenSearchBulkRequest(chunk).toBulkNdjson
              client.bulk(body, refresh = config.refreshPolicy)
            }
          }.flatTap { _ =>
            if (config.refreshPolicy.contains("true")) client.refreshIndex(store.writeIndexName) else Task.unit
          }
      }
      task.map(_ => bufferedOps.clear())
    }
  }

  override protected def _rollback: Task[Unit] = Task {
    bufferedOps.clear()
  }

  override protected def _close: Task[Unit] = Task.unit

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    Task.defer {
      prepared(query).flatMap { q =>
        val body = applyTrackTotalHits(q, searchBuilder.build(q))
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

    val baseBody = applyTrackTotalHits(q, searchBuilder.build(q.copy(sort = stableSort)))
    val body = searchAfter match {
      case Some(sa) =>
        baseBody match {
          case o: Obj => obj((o.value.toSeq :+ ("search_after" -> sa)): _*)
          case other => other
        }
      case None => baseBody
    }

    client.search(store.readIndexName, body).map { json =>
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

  override def truncate: Task[Int] =
    client.deleteByQuery(store.writeIndexName, obj("query" -> obj("match_all" -> obj())), refresh = refreshForDeleteByQuery)
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



