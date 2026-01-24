package lightdb.opensearch

import fabric._
import fabric.io.JsonFormatter
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.opensearch.query.OpenSearchDsl
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store
import rapid.Task
import rapid.taskSeq2Ops
import lightdb.opensearch.OpenSearchMetrics

/**
 * Rebuild helpers for offline migrations.
 *
 * These utilities are intended to support "new index + rebuild + alias swap" workflows where OpenSearch is treated
 * as a derived index (not the system of record).
 *
 * WARNING: These do not coordinate concurrent writes. For correctness under concurrent writes, callers must pause
 * writes or implement a dual-write + catch-up strategy before swapping aliases.
 */
object OpenSearchRebuild {
  case class RebuildDoc(id: String, source: Json, routing: Option[String] = None)

  object RebuildDoc {
    /**
     * Encodes a LightDB document using the same OpenSearch rules as transactional indexing.
     */
    def fromDoc[Doc <: Document[Doc]](doc: Doc,
                                      storeName: String,
                                      fields: List[Field[Doc, _]],
                                      config: OpenSearchConfig): RebuildDoc = {
      val prepared = OpenSearchDocEncoding.prepareForIndexing(
        doc = doc,
        storeName = storeName,
        fields = fields,
        config = config
      )
      RebuildDoc(id = doc._id.value, source = prepared.source, routing = prepared.routing)
    }
  }

  /**
   * A source-of-truth provider for rebuilds (typically one LightDB store).
   *
   * This exists to support:
   * - rebuilding a single OpenSearch collection from a system-of-record store
   * - rebuilding a join-domain index from multiple stores (parent + child types)
   */
  trait RebuildSource {
    def description: String

    /**
     * Streams docs from this source and indexes them into the specified OpenSearch index.
     */
    def indexInto(client: OpenSearchClient, index: String, bulkConfig: OpenSearchConfig): Task[Unit]
  }

  object RebuildSource {
    def fromStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](storeName: String,
                                                                     fields: List[Field[Doc, _]],
                                                                     source: Store[Doc, Model],
                                                                     docConfig: OpenSearchConfig): RebuildSource = new RebuildSource {
      override val description: String = s"store=${source.name}, type=$storeName"

      override def indexInto(client: OpenSearchClient, index: String, bulkConfig: OpenSearchConfig): Task[Unit] =
        source.transaction { tx =>
          val stream = tx.stream.map { d =>
            RebuildDoc.fromDoc(d, storeName = storeName, fields = fields, config = docConfig)
          }
          indexAll(client, index = index, docs = stream, config = bulkConfig)
        }
    }
  }

  /**
   * Catch-up iterator for coordinated rebuilds.
   *
   * - input: optional paging token (opaque to OpenSearchRebuild)
   * - output: None when done, else a batch with docs + nextToken
   */
  case class CatchUpBatch(docs: List[RebuildDoc], nextToken: Option[String])
  type CatchUpFn = Option[String] => Task[Option[CatchUpBatch]]

  /**
   * Pure helper: iterates a catch-up function and collects all batches (up to maxBatches).
   *
   * This does not talk to OpenSearch; it's intended for unit testing and for callers that want to preflight
   * their catch-up logic.
   */
  def collectCatchUpBatches(catchUp: CatchUpFn, maxBatches: Int): Task[List[CatchUpBatch]] = {
    val max = math.max(1, maxBatches)
    def loop(token: Option[String], n: Int, acc: List[CatchUpBatch]): Task[List[CatchUpBatch]] = {
      if n > max then {
        Task.error(new RuntimeException(s"OpenSearchRebuild catch-up exceeded maxCatchUpBatches=$max"))
      } else {
        catchUp(token).flatMap {
          case None => Task.pure(acc)
          case Some(batch) => loop(batch.nextToken, n + 1, acc :+ batch)
        }
      }
    }
    loop(token = None, n = 1, acc = Nil)
  }

  /**
   * Create a new physical index for the given read alias, bulk-index from the provided source stream, then atomically
   * repoint the read alias and optional write alias to the new index.
   *
   * @return the new physical index name
   */
  def rebuildAndRepointAliases(client: OpenSearchClient,
                               readAlias: String,
                               writeAlias: Option[String],
                               indexBody: Json,
                               docs: rapid.Stream[RebuildDoc],
                               config: OpenSearchConfig,
                               defaultSuffix: String = "_000001",
                               refreshAfter: Boolean = true): Task[String] = {
    client.aliasTargets(readAlias).flatMap { existing =>
      val nextIndex = OpenSearchIndexMigration.nextPhysicalIndexName(readAlias, existing, defaultSuffix = defaultSuffix)
      client.createIndex(nextIndex, indexBody)
        .next(indexAll(client, index = nextIndex, docs = docs, config = config))
        .next(if refreshAfter then client.refreshIndex(nextIndex) else Task.unit)
        .next(OpenSearchIndexMigration.repointReadWriteAliases(client, readAlias, writeAlias, nextIndex))
        .map(_ => nextIndex)
    }
  }

  /**
   * Create a new physical index, then index from multiple sources (sequentially), then repoint read/write aliases.
   *
   * This is the recommended primitive for join-domain rebuilds (parent + children) when OpenSearch is a derived index.
   *
   * @return the new physical index name
   */
  def rebuildAndRepointAliasesFromSources(client: OpenSearchClient,
                                          readAlias: String,
                                          writeAlias: Option[String],
                                          indexBody: Json,
                                          sources: List[RebuildSource],
                                          config: OpenSearchConfig,
                                          defaultSuffix: String = "_000001",
                                          refreshAfter: Boolean = true): Task[String] = {
    client.aliasTargets(readAlias).flatMap { existing =>
      val nextIndex = OpenSearchIndexMigration.nextPhysicalIndexName(readAlias, existing, defaultSuffix = defaultSuffix)
      client.createIndex(nextIndex, indexBody)
        .next {
          sources.foldLeft(Task.unit) { (acc, src) =>
            acc.next(src.indexInto(client, index = nextIndex, bulkConfig = config))
          }
        }
        .next(if refreshAfter then client.refreshIndex(nextIndex) else Task.unit)
        .next(OpenSearchIndexMigration.repointReadWriteAliases(client, readAlias, writeAlias, nextIndex))
        .map(_ => nextIndex)
    }
  }

  /**
   * Same as [[rebuildAndRepointAliases]] but accepts LightDB documents and encodes them using [[OpenSearchDocEncoding]].
   */
  def rebuildAndRepointAliasesFromDocs[Doc <: Document[Doc]](client: OpenSearchClient,
                                                             readAlias: String,
                                                             writeAlias: Option[String],
                                                             indexBody: Json,
                                                             storeName: String,
                                                             fields: List[Field[Doc, _]],
                                                             docs: rapid.Stream[Doc],
                                                             config: OpenSearchConfig,
                                                             defaultSuffix: String = "_000001",
                                                             refreshAfter: Boolean = true): Task[String] =
    rebuildAndRepointAliases(
      client = client,
      readAlias = readAlias,
      writeAlias = writeAlias,
      indexBody = indexBody,
      docs = docs.map(d => RebuildDoc.fromDoc(d, storeName = storeName, fields = fields, config = config)),
      config = config,
      defaultSuffix = defaultSuffix,
      refreshAfter = refreshAfter
    )

  /**
   * Rebuild from a source-of-truth LightDB store by streaming all docs from a read transaction and encoding them
   * using [[OpenSearchDocEncoding]].
   *
   * Note: the provided `storeName` + `fields` must match the target OpenSearch collection's indexing rules (e.g. for
   * join-domains, `storeName` is the join "type" for parent/child).
   */
  def rebuildAndRepointAliasesFromStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](client: OpenSearchClient,
                                                                                           readAlias: String,
                                                                                           writeAlias: Option[String],
                                                                                           indexBody: Json,
                                                                                           storeName: String,
                                                                                           fields: List[Field[Doc, _]],
                                                                                           source: Store[Doc, Model],
                                                                                           config: OpenSearchConfig,
                                                                                           defaultSuffix: String = "_000001",
                                                                                           refreshAfter: Boolean = true): Task[String] =
    source.transaction { tx =>
      rebuildAndRepointAliasesFromDocs(
        client = client,
        readAlias = readAlias,
        writeAlias = writeAlias,
        indexBody = indexBody,
        storeName = storeName,
        fields = fields,
        docs = tx.stream,
        config = config,
        defaultSuffix = defaultSuffix,
        refreshAfter = refreshAfter
      )
    }

  /**
   * Two-phase rebuild workflow (best-effort for concurrent writes):
   *
   * - create + rebuild new index
   * - switch the write alias to the new index (reads still served from the old read alias target)
   * - run a caller-provided catch-up indexing stream
   * - swap the read alias to the new index
   *
   * This allows the application to control correctness under concurrent writes by providing a catch-up stream
   * based on its system-of-record (e.g., "all changes since T0").
   *
   * @return the new physical index name
   */
  def rebuildSwitchWriteCatchUpSwapRead(client: OpenSearchClient,
                                        readAlias: String,
                                        writeAlias: String,
                                        indexBody: Json,
                                        initialDocs: rapid.Stream[RebuildDoc],
                                        catchUpDocs: Task[rapid.Stream[RebuildDoc]],
                                        config: OpenSearchConfig,
                                        defaultSuffix: String = "_000001",
                                        refreshAfterInitial: Boolean = true,
                                        refreshAfterCatchUp: Boolean = true): Task[String] = {
    // Single-shot wrapper: run at most one catch-up batch, then stop.
    @volatile var used = false
    rebuildSwitchWriteCatchUpLoopSwapRead(
      client = client,
      readAlias = readAlias,
      writeAlias = writeAlias,
      indexBody = indexBody,
      initialDocs = initialDocs,
      catchUp = _ =>
        if used then {
          Task.pure(None)
        } else {
          used = true
          catchUpDocs
            .flatMap(s => s.toList)
            .map { list =>
              if list.isEmpty then None else Some(CatchUpBatch(docs = list, nextToken = None))
            }
        },
      config = config,
      defaultSuffix = defaultSuffix,
      refreshAfterInitial = refreshAfterInitial,
      refreshBetweenCatchUpBatches = false,
      refreshAfterCatchUp = refreshAfterCatchUp,
      maxCatchUpBatches = 1
    )
  }

  /**
   * Two-phase rebuild with iterative catch-up batches (recommended building block for coordinated rebuilds).
   *
   * - rebuild new index from `initialDocs`
   * - repoint write alias to the new index
   * - run catch-up batches via `catchUp` until it returns None
   * - swap read alias to the new index
   *
   * Correctness note: this still relies on the caller to provide a sound catch-up mechanism (usually based on a
   * system-of-record changelog and a checkpoint captured before switching the write alias).
   */
  def rebuildSwitchWriteCatchUpLoopSwapRead(client: OpenSearchClient,
                                            readAlias: String,
                                            writeAlias: String,
                                            indexBody: Json,
                                            initialDocs: rapid.Stream[RebuildDoc],
                                            catchUp: CatchUpFn,
                                            config: OpenSearchConfig,
                                            defaultSuffix: String = "_000001",
                                            refreshAfterInitial: Boolean = true,
                                            refreshBetweenCatchUpBatches: Boolean = false,
                                            refreshAfterCatchUp: Boolean = true,
                                            maxCatchUpBatches: Int = 1000): Task[String] = {
    client.aliasTargets(readAlias).flatMap { existing =>
      val nextIndex = OpenSearchIndexMigration.nextPhysicalIndexName(readAlias, existing, defaultSuffix = defaultSuffix)
      client.createIndex(nextIndex, indexBody)
        .next(indexAll(client, index = nextIndex, docs = initialDocs, config = config))
        .next(if refreshAfterInitial then client.refreshIndex(nextIndex) else Task.unit)
        .next(repointWriteAlias(client, writeAlias = writeAlias, targetIndex = nextIndex))
        .next(runCatchUpBatches(client, index = nextIndex, catchUp = catchUp, config = config, refreshBetween = refreshBetweenCatchUpBatches, maxBatches = maxCatchUpBatches))
        .next(if refreshAfterCatchUp then client.refreshIndex(nextIndex) else Task.unit)
        .next(OpenSearchIndexMigration.repointAlias(client, alias = readAlias, targetIndex = nextIndex))
        .map(_ => nextIndex)
    }
  }

  private def runCatchUpBatches(client: OpenSearchClient,
                                index: String,
                                catchUp: CatchUpFn,
                                config: OpenSearchConfig,
                                refreshBetween: Boolean,
                                maxBatches: Int): Task[Unit] = {
    val max = math.max(1, maxBatches)
    def loop(token: Option[String], n: Int): Task[Unit] = {
      if n > max then {
        Task.error(new RuntimeException(s"OpenSearchRebuild catch-up exceeded maxCatchUpBatches=$max"))
      } else {
        catchUp(token).flatMap {
          case None =>
            Task.unit
          case Some(batch) =>
            val docs = rapid.Stream.emits(batch.docs)
            indexAll(client, index = index, docs = docs, config = config)
              .next(if refreshBetween then client.refreshIndex(index) else Task.unit)
              .next(loop(batch.nextToken, n + 1))
        }
      }
    }
    loop(token = None, n = 1)
  }

  /**
   * Partial rebuild in-place: delete a subset of documents by query, then index replacement docs.
   *
   * This is useful for incremental rebuilds like:
   * - rebuild by parent id (delete parent + children, then reindex them)
   * - rebuild by time window (delete docs whose updated timestamp is within a range, then reindex)
   *
   * @return number of deleted documents (as reported by OpenSearch)
   */
  def rebuildSubsetInIndex(client: OpenSearchClient,
                           index: String,
                           deleteQuery: Json,
                           docs: rapid.Stream[RebuildDoc],
                           config: OpenSearchConfig,
                           refreshAfter: Boolean = true): Task[Int] = {
    client
      .deleteByQuery(index, obj("query" -> deleteQuery), refresh = Some("false"))
      .flatMap { deleted =>
        indexAll(client, index = index, docs = docs, config = config)
          .next(if refreshAfter then client.refreshIndex(index) else Task.unit)
          .map(_ => deleted)
      }
  }

  /**
   * Convenience helper for join-domains: delete a parent + its children (by parent ids) and then index replacement docs.
   *
   * @param index            join-domain index or alias
   * @param joinFieldName    join field name (default: "__lightdb_join")
   * @param parentStoreName  join parent "type" (typically the parent store name)
   * @param childParentFields map: childStoreName -> child field name containing parent id (for delete query)
   */
  def rebuildJoinDomainByParentIds(client: OpenSearchClient,
                                   index: String,
                                   joinFieldName: String,
                                   parentStoreName: String,
                                   childParentFields: Map[String, String],
                                   parentIds: List[String],
                                   docs: rapid.Stream[RebuildDoc],
                                   config: OpenSearchConfig,
                                   refreshAfter: Boolean = true): Task[Int] = {
    val deleteQuery = joinDomainDeleteQueryByParentIds(
      joinFieldName = joinFieldName,
      parentStoreName = parentStoreName,
      childParentFields = childParentFields,
      parentIds = parentIds
    )
    rebuildSubsetInIndex(
      client = client,
      index = index,
      deleteQuery = deleteQuery,
      docs = docs,
      config = config,
      refreshAfter = refreshAfter
    )
  }

  /**
   * Builds a join-domain delete query that deletes:
   * - parents with ids in parentIds (joinFieldName == parentStoreName)
   * - children whose joinParentField is in parentIds (joinFieldName == childStoreName)
   */
  def joinDomainDeleteQueryByParentIds(joinFieldName: String,
                                       parentStoreName: String,
                                       childParentFields: Map[String, String],
                                       parentIds: List[String]): Json = {
    val ids = parentIds.distinct
    if ids.isEmpty then {
      // No-op: delete nothing.
      obj("bool" -> obj("must_not" -> arr(obj("match_all" -> obj()))))
    } else {
      val parentClause = OpenSearchDsl.boolQuery(
        must = List(
          OpenSearchDsl.term(joinFieldName, str(parentStoreName)),
          obj("ids" -> obj("values" -> arr(ids.map(str): _*)))
        )
      )
      val childClauses = childParentFields.toList.map { case (childStoreName, parentFieldName) =>
        OpenSearchDsl.boolQuery(
          must = List(
            OpenSearchDsl.term(joinFieldName, str(childStoreName)),
            OpenSearchDsl.terms(parentFieldName, ids.map(str))
          )
        )
      }
      OpenSearchDsl.boolQuery(
        should = parentClause :: childClauses,
        minimumShouldMatch = Some(1)
      )
    }
  }

  private def indexAll(client: OpenSearchClient,
                       index: String,
                       docs: rapid.Stream[RebuildDoc],
                       config: OpenSearchConfig): Task[Unit] = {
    def withIngestPermit[A](task: => Task[A]): Task[A] =
      OpenSearchIngestLimiter.withPermit(
        key = config.normalizedBaseUrl,
        maxConcurrent = config.ingestMaxConcurrentRequests
      )(task)

    docs
      .chunk(math.max(1, config.bulkMaxDocs))
      .evalMap { chunk =>
        val list = chunk.toList
        val ops = list.map(d => OpenSearchBulkOp.index(index, d.id, d.source, routing = d.routing))
        val chunks = splitByBytes(ops, maxDocs = config.bulkMaxDocs, maxBytes = config.bulkMaxBytes)
        val chunkTasks = chunks.map { c =>
          Task.defer {
            val body = OpenSearchBulkRequest(c).toBulkNdjson
            if config.metricsEnabled then {
              OpenSearchMetrics.recordBulkAttempt(config.normalizedBaseUrl, docs = c.size, bytes = body.length)
            }
            withIngestPermit {
              client.bulk(body, refresh = None)
            }
          }
        }
        if config.bulkConcurrency <= 1 || chunkTasks.size <= 1 then {
          chunkTasks.foldLeft(Task.unit)((acc, t) => acc.next(t))
        } else {
          // bounded parallelism across bulk chunks (still protected by global ingest limiter)
          chunkTasks.tasksPar.unit
        }
      }
      .drain
  }

  private def repointWriteAlias(client: OpenSearchClient, writeAlias: String, targetIndex: String): Task[Unit] =
    client.aliasTargets(writeAlias).flatMap { existing =>
      val removes = existing.map(idx => obj("remove" -> obj("index" -> str(idx), "alias" -> str(writeAlias))))
      val add = obj("add" -> obj("index" -> str(targetIndex), "alias" -> str(writeAlias), "is_write_index" -> bool(true)))
      client.updateAliases(obj("actions" -> arr((removes :+ add): _*)))
    }

  private def splitByBytes(ops: List[OpenSearchBulkOp],
                           maxDocs: Int,
                           maxBytes: Int): List[List[OpenSearchBulkOp]] = {
    val md = math.max(1, maxDocs)
    val mb = math.max(1, maxBytes)
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

    ops.foreach { op =>
      val b = estimateBytes(op)
      val wouldExceedDocs = current.nonEmpty && current.size >= md
      val wouldExceedBytes = current.nonEmpty && (currentBytes + b) > mb
      if wouldExceedDocs || wouldExceedBytes then {
        chunks += current.toList
        current.clear()
        currentBytes = 0
      }
      current += op
      currentBytes += b
    }

    if current.nonEmpty then chunks += current.toList
    chunks.toList
  }
}


