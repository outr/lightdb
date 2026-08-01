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
   * Materializes only the id list, then rewrites the documents in short, independent per-batch
   * transactions, so it scales to very large collections with bounded memory AND never holds a
   * long-lived read transaction open across its own writes (see the note in the body). This is a
   * maintenance operation, not a hot path.
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
      // Materialize the ID LIST in one short read transaction, then rewrite the documents in independent
      // batches, each in its OWN transaction. The previous shape (a single transaction streaming reads
      // WHILE upserting back into the same store) held a long-lived read transaction open across every
      // write batch; on SQL backends the reads and writes land on different pooled connections, so an
      // uncommitted batch could hold row locks a later batch needed while the application waited on that
      // batch to advance the stream — a circular wait split across the JVM and the database, invisible
      // to the database's deadlock detector (it only sees one waiter), so it stalled forever instead of
      // aborting. Ids-only materialization keeps memory bounded for very large collections.
      val batchSize = commitEvery.getOrElse(1_000)
      transaction { tx =>
        tx.query.materialized(m => List(m._id)).toList.map(_.map(mi => mi(_._id)))
      }.flatMap { ids =>
        val total = ids.size
        val counter = new AtomicInteger(0)
        ids.grouped(batchSize).toList.foldLeft(Task.unit) { (acc, batch) =>
          acc.flatMap { _ =>
            transaction { tx =>
              tx.query.filter(_._id.in(batch)).toList.flatMap(docs => tx.upsert(docs))
            }.map { docs =>
              val current = counter.addAndGet(docs.size)
              progressManager.percentage(
                current = current,
                total = total,
                message = Some(s"Re-Indexing $name: $current of $total")
              )
            }
          }
        }.map(_ => true)
      }
    }

  /** Re-writes a single document so its derived/indexed projections are recomputed. */
  override def reIndexDoc(doc: Doc): Task[Boolean] = transaction { tx =>
    tx.upsert(doc).map(_ => true)
  }
}
