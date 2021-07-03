package lightdb.store.halo

import lightdb.collection.Collection
import lightdb.store.{ObjectStore, ObjectStoreSupport}
import lightdb.{Document, LightDB}

import java.nio.file.Paths

/**
 * SharedHaloSupport uses a single HaloStore instance across all collections
 */
trait SharedHaloSupport extends ObjectStoreSupport {
  this: LightDB =>

  protected def haloIndexThreads: Int = 2
  protected def haloMaxFileSize: Int = 1024 * 1024

  private lazy val shared: HaloStore = HaloStore(
    directory = directory.getOrElse(Paths.get("db")).resolve("store"),
    indexThreads = haloIndexThreads,
    maxFileSize = haloMaxFileSize
  )

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = shared
}