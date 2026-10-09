package lightdb.store.journal

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.batch.BatchConfig
import rapid.Task

import java.nio.file.Path

/** A collection of a [[JournalCollectionManager]]: `underlying` (a collection of the wrapped manager) made joinable
  * by `journal`. Queries read its committed rows. */
class JournalCollection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           val underlying: Collection[Doc, Model],
                                                                           val journal: Journal,
                                                                           val maxOpBytes: Long,
                                                                           val storeMode: StoreMode[Doc, Model],
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager) with JournalBacked[Doc, Model] {
  override type TX = JournalCollectionTransaction[Doc, Model]

  journal.register(this)

  override def supportsNativeExistsChild: Boolean = underlying.supportsNativeExistsChild

  override def supportsNestedQueries: Boolean = underlying.supportsNestedQueries

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]): Task[TX] =
    Task(JournalCollectionTransaction(this, parent, writeHandlerFactory))

  override protected def createWriteHandler(transaction: Transaction[Doc, Model], config: BatchConfig): WriteHandler[Doc, Model] =
    new JournalWriteHandler(transaction, this, ops => transaction.asInstanceOf[JournalTransactionSupport[Doc, Model]].commitPartial(ops))

  override def reIndexDoc(doc: Doc): Task[Boolean] = underlying.reIndexDoc(doc)
}
