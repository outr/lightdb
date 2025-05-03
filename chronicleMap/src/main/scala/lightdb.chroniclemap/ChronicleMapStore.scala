package lightdb.chroniclemap

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import net.openhft.chronicle.map.ChronicleMap
import rapid.Task
import scribe.{Level, Logger}

import java.nio.file.{Files, Path}

class ChronicleMapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           val storeMode: StoreMode[Doc, Model],
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  sys.props("net.openhft.chronicle.hash.impl.util.jna.PosixFallocate.fallocate") = "false"

  override type TX = ChronicleMapTransaction[Doc, Model]

  private lazy val db: ChronicleMap[String, String] = {
    val b = ChronicleMap
      .of(classOf[String], classOf[String])
      .name(name)
      .entries(1_000_000)
      .averageKeySize(32)
      .averageValueSize(5 * 1024)
      .maxBloatFactor(2.0)
      .sparseFile(true)
    path match {
      case Some(d) =>
        Files.createDirectories(d.getParent)
        b.createPersistedTo(d.toFile)
      case None => b.create()
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task(ChronicleMapTransaction(this, db, parent))

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(db))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    db.close()
  })
}

object ChronicleMapStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = ChronicleMapStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    Logger("net.openhft.chronicle").withMinimumLevel(Level.Warn).replace()

    new ChronicleMapStore[Doc, Model](name, path, model, storeMode, db, this)
  }
}
