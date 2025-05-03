package lightdb.lmdb

import fabric.{Json, Null}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.{Store, StoreManager, StoreMode, WriteBuffer, WriteOp}
import lightdb.transaction.Transaction
import lightdb.{Id, LightDB}
import org.lmdbjava._
import rapid._

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LMDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                   path: Option[Path],
                                                                   model: Model,
                                                                   private[lmdb] val instance: LMDBInstance,
                                                                   val storeMode: StoreMode[Doc, Model],
                                                                   db: LightDB,
                                                                   storeManager: StoreManager) extends Store[Doc, Model](name, path, model, db, storeManager) {
  override type TX = LMDBTransaction[Doc, Model]

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    LMDBTransaction(this, instance.env, parent)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().map { _ =>
    instance.env.sync(true)
    instance.dbi.close()
    instance.env.close()
  }
}

object LMDBStore extends StoreManager {
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

  private[lmdb] val keyBufferPool = new ByteBufferPool(512)
  private[lmdb] val valueBufferPool = new ByteBufferPool(512)

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
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    }
    val env = Env
      .create()
      .setMaxDbs(MaxDbs)
      .setMapSize(MapSize)
      .setMaxReaders(MaxReaders)
      .open(path.toFile, EnvFlags.MDB_WRITEMAP)
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
