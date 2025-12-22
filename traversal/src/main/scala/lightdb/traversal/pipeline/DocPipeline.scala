package lightdb.traversal.pipeline

import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.traversal.store.{TraversalIndexCache, TraversalQueryEngine}
import lightdb.transaction.PrefixScanningTransaction
import profig.Profig
import fabric.rw._
import rapid._

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
  def `match`(filter: Filter[Doc]): DocPipeline[Doc, Model] = {
    val seeded: Stream[Doc] = Stream.force {
      // Avoid any materialization unless explicitly requested:
      // - in-memory cache is opt-in
      // - otherwise, prefer persisted postings if enabled on the underlying TraversalTransaction
      val seedIdsTask: Task[Option[Set[lightdb.id.Id[Doc]]]] =
        if (indexCache.enabled) {
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

  def `match`(f: Model => Filter[Doc]): DocPipeline[Doc, Model] = `match`(f(model))

  def project[A](f: Doc => A): Pipeline[A] = Pipeline(stream.map(f))

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


