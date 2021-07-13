package lightdb.store.halo

import lightdb.collection.Collection
import lightdb.store.ObjectStore
import lightdb.{Document, LightDB}

import java.nio.file.Paths

/**
 * SharedHaloSupport uses a single HaloStore instance across all collections
 */
trait SharedHaloSupport extends HaloSupport {
  this: LightDB =>

  private lazy val shared: HaloStore = createStore(directory.getOrElse(Paths.get("db")).resolve("store"))

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = shared
}