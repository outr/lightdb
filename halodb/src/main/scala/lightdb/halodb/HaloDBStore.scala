package lightdb.halodb

import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.*
import scribe.{Level, Logger}

import java.nio.file.Path
import scala.language.implicitConversions

class HaloDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     instance: HaloDBInstance,
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = HaloDBTransaction[Doc, Model]

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(HaloDBTransaction(this, instance, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(instance.dispose())
}

object HaloDBStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = HaloDBStore[Doc, Model]

  Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): HaloDBStore[Doc, Model] = {
    val instance = new DirectHaloDBInstance(path.get)
    new HaloDBStore[Doc, Model](name, path, model, storeMode, instance, db, this)
  }
}
