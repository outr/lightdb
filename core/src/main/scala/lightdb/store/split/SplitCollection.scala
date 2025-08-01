package lightdb.store.split

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.{Task, logger}

import java.nio.file.Path
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

  override protected def initialize(): Task[Unit] = storage.init.and(searching.init).next(super.initialize())

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = for {
    t <- Task(SplitCollectionTransaction(this, parent))
    t1 <- storage.transaction.create(Some(t))
    t2 <- searching.transaction.create(Some(t))
    _ = t._storage = t1.asInstanceOf[t.store.storage.TX]
    _ = t._searching = t2.asInstanceOf[t.store.searching.TX]
  } yield t

  override def verify(): Task[Boolean] = transaction { transaction =>
    for {
      storageCount <- transaction.storage.count
      searchCount <- transaction.searching.count
      shouldReIndex = storageCount != searchCount && model.fields.count(_.indexed) > 1
      _ <- logger.warn(s"$name out of sync! Storage Count: $storageCount, Search Count: $searchCount. Re-Indexing...")
        .next(reIndexInternal(transaction))
        .next(logger.info(s"$name re-indexed successfully!"))
        .when(shouldReIndex)
    } yield shouldReIndex
  }

  override def reIndex(): Task[Boolean] = transaction { transaction =>
    reIndexInternal(transaction).map(_ => true)
  }

  override def reIndex(doc: Doc): Task[Boolean] = transaction { transaction =>
    transaction.upsert(doc).map(_ => true)
  }

  override def optimize(): Task[Unit] = searching.optimize().next(storage.optimize())

  private def reIndexInternal(transaction: TX): Task[Unit] = transaction
    .searching
    .truncate
    .flatMap { _ =>
      transaction
        .storage
        .stream
        .chunk(SplitCollection.ReIndexChunkSize)
        .par(SplitCollection.ReIndexMaxThreads) { docs =>
          transaction.searching.insert(docs)
        }
        .drain
    }

  override protected def doDispose(): Task[Unit] = storage.dispose.and(searching.dispose).next(super.doDispose())
}

object SplitCollection {
  var ReIndexMaxThreads: Int = 32
  var ReIndexChunkSize: Int = 128
}