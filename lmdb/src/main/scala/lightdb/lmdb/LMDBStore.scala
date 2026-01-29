package lightdb.lmdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreMode}
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import lightdb.store.write.WriteOp
import org.lmdbjava.*
import rapid.*

import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LMDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                   path: Option[Path],
                                                                   model: Model,
                                                                   private[lmdb] val instance: LMDBInstance,
                                                                   val storeMode: StoreMode[Doc, Model],
                                                                   db: LightDB,
                                                                   storeManager: PrefixScanningStoreManager) extends Store[Doc, Model](name, path, model, db, storeManager) with PrefixScanningStore[Doc, Model] {
  override type TX = LMDBTransaction[Doc, Model]

  override def defaultBatchConfig: BatchConfig = BatchConfig.Buffered()

  override protected def flushOps(transaction: Transaction[Doc, Model], ops: Seq[WriteOp[Doc]]): Task[Unit] =
    transaction.asInstanceOf[TX].flushOps(ops)

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = Task {
    LMDBTransaction(this, instance, parent, writeHandlerFactory)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().map { _ =>
    instance.close()
  }
}

object LMDBStore extends PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = LMDBStore[Doc, Model]

  /**
   * Maximum number of collections. Defaults to 1,000
   */
  var MaxDbs: Int = 1_000

  /**
   * Map Size. Defaults to 100gig
   */
  var MapSize: Long = 100L * 1024 * 1024 * 1024

  /**
   * Max Readers. Defaults to 128
   */
  var MaxReaders: Int = 128

  private var instances = Map.empty[LightDB, LMDBInstance]

  def instance(db: LightDB, path: Path): LMDBInstance = synchronized {
    instances.get(db) match {
      case Some(instance) => instance
      case None =>
        val instance = createInstance(path)
        instances += db -> instance
        instance
    }
  }

  private def createInstance(path: Path): LMDBInstance = {
    if !Files.exists(path) then {
      Files.createDirectories(path)
    }
    val env = Env
      .create()
      .setMaxDbs(MaxDbs)
      .setMapSize(MapSize)
      .setMaxReaders(MaxReaders)
      .open(path.toFile) // default flags for safety (no WRITEMAP)
    LMDBInstance(env, env.openDbi(name, DbiFlags.MDB_CREATE))
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    new LMDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      instance = createInstance(path.get),
      storeMode = storeMode,
      db = db,
      storeManager = this
    )
  }
}
