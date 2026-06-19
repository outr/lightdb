package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.progress.ProgressManager
import lightdb.transaction.CollectionTransaction
import rapid.Task

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

abstract class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                             path: Option[Path],
                                                                             model: Model,
                                                                             lightDB: LightDB,
                                                                             storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX <: CollectionTransaction[Doc, Model]

  /**
   * Rebuilds derived/indexed data for a full (`StoreMode.All`) collection by re-writing every stored
   * document. A field's value is produced by its getter at write time, so a newly-added computed or
   * indexed field (e.g. `field.index(d => s"${d.name}.${d.age}")`) is `NULL`/absent for rows written
   * before it existed; re-writing each document recomputes those projections and refreshes the index.
   * Call this once after adding such a field.
   *
   * Streams the documents straight from the read into the upsert (no materialization), so it scales to
   * very large collections with bounded memory. This is a maintenance operation, not a hot path.
   *
   * Stores whose documents live elsewhere (`StoreMode.Indexes` / split storage) cannot rebuild from
   * themselves and override this — see [[lightdb.store.split.SplitCollection]], which re-derives the
   * index from the backing storage instead.
   */
  override def reIndex(progressManager: ProgressManager = ProgressManager.none,
                       commitEvery: Option[Int] = None): Task[Boolean] =
    if !storeMode.isAll then {
      super.reIndex(progressManager, commitEvery)
    } else {
      transaction { tx =>
        tx.count.flatMap { total =>
          val counter = new AtomicInteger(0)
          val stream = tx.stream.evalTap { _ =>
            Task {
              val current = counter.incrementAndGet()
              progressManager.percentage(
                current = current,
                total = total,
                message = Some(s"Re-Indexing $name: $current of $total")
              )
            }
          }
          tx.upsert(stream, commitEvery)
        }.map(_ => true)
      }
    }

  /** Re-writes a single document so its derived/indexed projections are recomputed. */
  override def reIndexDoc(doc: Doc): Task[Boolean] = transaction { tx =>
    tx.upsert(doc).map(_ => true)
  }
}
