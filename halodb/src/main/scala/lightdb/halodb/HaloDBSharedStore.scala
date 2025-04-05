package lightdb.halodb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{StoreManager, StoreMode}

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

case class HaloDBSharedStore(directory: Path, useNameAsPrefix: Boolean = false) extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = HaloDBStore[Doc, Model]

  private lazy val instance = new DirectHaloDBInstance(directory)

  private val prefixes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toVector

  private val counter = new AtomicInteger(0)
  private var prefixMap = Map.empty[String, String]

  private def prefixFor(db: LightDB, name: String): String = if (useNameAsPrefix) {
    name
  } else {
    synchronized {
      prefixMap.get(name) match {
        case Some(prefix) => prefix
        case None =>
          val i = counter.incrementAndGet()
          val prefix = prefixes(i).toString
          prefixMap += name -> prefix
          prefix
      }
    }
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new HaloDBStore[Doc, Model](name, path, model, storeMode, SharedHaloDBInstance(instance, prefixFor(db, name)), db, this)
}
