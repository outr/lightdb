package lightdb.store.split

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.progress.ProgressManager
import lightdb.store.{Collection, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.{Task, logger}

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import scala.language.implicitConversions

class SplitCollection[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model]
](override val name: String,
  path: Option[Path],
  model: Model,
  val storage: Storage,
  val searching: Searching,
  val storeMode: StoreMode[Doc, Model],
  db: LightDB,
  storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, db, storeManager) {
  override type TX = SplitCollectionTransaction[Doc, Model, Storage, Searching]

  /**
   * Delegate native join support to the searching collection.
   *
   * This is critical when using SplitCollection as "system of record + derived search index", e.g.
   * RocksDB (storage) + OpenSearch (searching). Without this, ExistsChild will be forced through the planner
   * fallback (materialize parent ids), even though the searching backend can execute joins natively.
   */
  override def supportsNativeExistsChild: Boolean = searching.supportsNativeExistsChild

  override protected def initialize(): Task[Unit] = storage.init.and(searching.init).next(super.initialize())

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = for {
    t <- Task(SplitCollectionTransaction(this, parent))
    t1 <- storage.transaction.create(Some(t))
    t2 <- searching.transaction.create(Some(t))
    _ = t._storage = t1.asInstanceOf[t.store.storage.TX]
    _ = t._searching = t2.asInstanceOf[t.store.searching.TX]
  } yield t

  override def verify(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = transaction { transaction =>
    for {
      storageCount <- transaction.storage.count
      searchCount <- transaction.searching.count
      shouldReIndex = storageCount != searchCount && model.fields.count(_.indexed) > 1 && SplitCollection.ReIndexWhenOutOfSync
      _ <- logger.warn(s"$name out of sync! Storage Count: $storageCount, Search Count: $searchCount. Re-Indexing...")
        .next(reIndexInternal(transaction, progressManager))
        .next(logger.info(s"$name re-indexed successfully!"))
        .when(shouldReIndex)
    } yield shouldReIndex
  }

  override def reIndex(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = transaction { transaction =>
    reIndexInternal(transaction, progressManager).map(_ => true)
  }

  override def reIndexDoc(doc: Doc): Task[Boolean] = transaction { transaction =>
    transaction.upsert(doc).map(_ => true)
  }

  override def optimize(): Task[Unit] = searching.optimize().next(storage.optimize())

  private def reIndexInternal(transaction: TX, progressManager: ProgressManager): Task[Unit] = transaction.searching.truncate.flatMap { _ =>
    transaction.storage.count.flatMap { total =>
      val counter = new AtomicInteger(0)
      transaction.searching.insert(transaction.storage.stream.evalTap { _ =>
        Task {
          val count = counter.incrementAndGet()
          progressManager.percentage(
            current = count,
            total = total,
            message = Some(s"Re-Indexing $name: $count of $total")
          )
        }
      }).unit
    }
  }

  override protected def doDispose(): Task[Unit] = storage.dispose.and(searching.dispose).next(super.doDispose())
}

object SplitCollection {
  var ReIndexWhenOutOfSync: Boolean = true
}