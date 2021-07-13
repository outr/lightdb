package lightdb.store.halo

import lightdb.{Document, LightDB}
import lightdb.collection.Collection
import lightdb.store.ObjectStore

import java.nio.file.Paths

/**
 * MultiHaloSupport creates a new HaloStore directory per collection
 */
trait MultiHaloSupport extends HaloSupport {
  this: LightDB =>

  private var map = Map.empty[String, ObjectStore]

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = {
    val store = map.getOrElse(collection.collectionName, createStore(directory.getOrElse(Paths.get("db")).resolve(collection.collectionName).resolve("store")))
    synchronized {
      map += collection.collectionName -> store
    }
    store
  }
}