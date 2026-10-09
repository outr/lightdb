package lightdb.store.journal

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.batch.BatchConfig
import rapid.Task

import java.nio.file.Path

/** A store of a [[JournalStoreManager]]: `underlying` (a store of the wrapped manager) made joinable by `journal`. */
class JournalStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      val underlying: Store[Doc, Model],
                                                                      val journal: Journal,
                                                                      val maxOpBytes: Long,
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      lightDB: LightDB,
                                                                      storeManager: StoreManager)
  extends Store[Doc, Model](name, path, model, lightDB, storeManager) with JournalBacked[Doc, Model] {
  override type TX = JournalTransaction[Doc, Model]

  journal.register(this)

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]): Task[TX] =
    Task(JournalTransaction(this, parent, writeHandlerFactory))

  override protected def createWriteHandler(transaction: Transaction[Doc, Model], config: BatchConfig): WriteHandler[Doc, Model] =
    new JournalWriteHandler(transaction, this, ops => transaction.asInstanceOf[JournalTransactionSupport[Doc, Model]].commitPartial(ops))
}
