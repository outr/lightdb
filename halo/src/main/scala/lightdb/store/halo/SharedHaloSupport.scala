package lightdb.store.halo

import lightdb.collection.Collection
import lightdb.store.{ObjectStore, ObjectStoreSupport}
import lightdb.{Document, LightDB}

import java.nio.file.Paths

trait SharedHaloSupport extends ObjectStoreSupport {
  this: LightDB =>

  private lazy val shared: HaloStore = new HaloStore(directory.getOrElse(Paths.get("db")).resolve("store"))

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = shared
}