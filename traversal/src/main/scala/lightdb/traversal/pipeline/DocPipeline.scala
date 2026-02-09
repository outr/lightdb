package lightdb.traversal.pipeline

import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.graph.EdgeDocument
import lightdb.traversal.graph.{EdgeTraversalBuilder, TraversalStrategy}
import lightdb.traversal.store.{TraversalIndexCache, TraversalQueryEngine}
import lightdb.transaction.PrefixScanningTransaction
import profig.Profig
import fabric.rw.*
import rapid.*

/**
 * A typed, Doc-aware pipeline that can leverage traversal candidate seeding for Match-like stages.
 *
 * This is the bridge between:
 * - MongoDB-style aggregation pipeline ergonomics
 * - LightDB's typed models/fields/filters
 * - traversal's execution primitives (candidate seeding + verification)
 */
final case class DocPipeline[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  storeName: String,
  model: Model,
  tx: PrefixScanningTransaction[Doc, Model],
  indexCache: TraversalIndexCache[Doc, Model],
  stream: Stream[Doc]
) {
  def filter(f: Model => Filter[Doc]): DocPipeline[Doc, Model] = `match`(f)
  def `match`(f: Model => Filter[Doc]): DocPipeline[Doc, Model] = `match`(f(model))
  def `match`(filter: Filter[Doc]): DocPipeline[Doc, Model] = {
    val seeded: Stream[Doc] = Stream.force {
      // Avoid any materialization unless explicitly requested:
      // - in-memory cache is opt-in
      // - otherwise, prefer persisted postings if enabled on the underlying TraversalTransaction
      val seedIdsTask: Task[Option[Set[lightdb.id.Id[Doc]]]] =
        if indexCache.enabled then {
          tx.stream.toList.map { docs =>
            indexCache.ensureBuilt(docs.iterator)
            TraversalQueryEngine
              .seedCandidatesForPipeline(storeName, model, Some(filter), indexCache)
              .map(_.map(id => lightdb.id.Id[Doc](id)).toSet)
          }
        } else {
          tx match {
            case tt: lightdb.traversal.store.TraversalTransaction[Doc @unchecked, Model @unchecked]
              if tt.store.persistedIndexEnabled && tt.store.name != "_backingStore" =>
              tt.store.effectiveIndexBacking match {
                case Some(idx) =>
                  idx.transaction { kv =>
                    val kvTx = kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[lightdb.KeyValue, lightdb.KeyValue.type]]
                    TraversalQueryEngine
                      .seedCandidatesForPipelinePersisted(storeName, model, Some(filter), kvTx)
                      .map(_.map(_.map(id => lightdb.id.Id[Doc](id)).toSet))
                  }
                case None =>
                  Task.pure(None)
              }
            case _ =>
              Task.pure(None)
          }
        }

      seedIdsTask.map { seedIds =>
        val base = seedIds match {
          case Some(ids) =>
            Stream
              .emits(ids.toList)
              .evalMap(id => tx.get(id))
              .filter(_.nonEmpty)
              .map(_.get)
          case None =>
            tx.stream
        }

        val state = new lightdb.field.IndexingState
        base.filter(doc => TraversalQueryEngine.evalFilter(filter, model, doc, state))
      }
    }

    copy(stream = seeded)
  }

  def project[A](f: Doc => A): Pipeline[A] = Pipeline(stream.map(f))

  /**
   * Hop from current documents across edges to reach target documents, returning a new pipeline.
   *
   * This bridges `DocPipeline` directly into `EdgeTraversalBuilder`, enabling type-safe
   * graph traversal within the pipeline pattern:
   *
   * {{{
   * personPipeline
   *   .filter(_.age >= 18)
   *   .hop(friendEdgeTx, personTx, maxDepth = 2)
   *   .filter(_.city === "Austin")
   *   .toList
   * }}}
   *
   * @param edgeTx     Transaction for the edge collection
   * @param targetTx   Transaction for the target document collection
   * @param maxDepth   Maximum traversal depth (default: 1 for single-hop)
   * @param edgeFilter Optional predicate to filter edges during traversal
   * @param strategy   Traversal strategy (BFS or DFS, default: BFS)
   */
  def hop[E <: EdgeDocument[E, Doc, T], T <: Document[T], TModel <: DocumentModel[T]](
    edgeTx: PrefixScanningTransaction[E, _],
    targetTx: PrefixScanningTransaction[T, TModel],
    maxDepth: Int = 1,
    edgeFilter: E => Boolean = (_: E) => true,
    strategy: TraversalStrategy = TraversalStrategy.BFS
  ): DocPipeline[T, TModel] = {
    val targetIds: Stream[lightdb.id.Id[T]] = stream.flatMap { doc =>
      EdgeTraversalBuilder(
        fromIds = Stream.emit(doc._id.asInstanceOf[lightdb.id.Id[Doc]].coerce),
        tx = edgeTx,
        maxDepth = maxDepth,
        edgeFilter = edgeFilter,
        strategy = strategy
      ).edges.map(_._to)
    }.distinct

    val targetStream: Stream[T] = targetIds.evalMap(id => targetTx.get(id)).collect { case Some(doc) => doc }

    DocPipeline(
      storeName = targetTx.store.name,
      model = targetTx.store.model,
      tx = targetTx,
      indexCache = new TraversalIndexCache[T, TModel](
        storeName = targetTx.store.name,
        model = targetTx.store.model,
        enabled = false
      ),
      stream = targetStream
    )
  }

  def toList: Task[List[Doc]] = stream.toList
  def count: Task[Long] = stream.count.map(_.toLong)
}

object DocPipeline {
  def fromTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    tx: PrefixScanningTransaction[Doc, Model]
  ): DocPipeline[Doc, Model] =
    DocPipeline(
      tx.store.name,
      tx.store.model,
      tx,
      new TraversalIndexCache[Doc, Model](
        storeName = tx.store.name,
        model = tx.store.model,
        enabled = Profig("lightdb.traversal.indexCache").opt[Boolean].getOrElse(false)
      ),
      tx.stream
    )

  def fromTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    tx: PrefixScanningTransaction[Doc, Model],
    indexCache: TraversalIndexCache[Doc, Model]
  ): DocPipeline[Doc, Model] =
    DocPipeline(tx.store.name, tx.store.model, tx, indexCache, tx.stream)
}


