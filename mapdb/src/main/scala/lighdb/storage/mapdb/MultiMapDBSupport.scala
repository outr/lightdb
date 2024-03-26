package lighdb.storage.mapdb

import lightdb.collection.Collection
import lightdb.store.{ObjectStore, ObjectStoreSupport}
import lightdb.{Document, LightDB}

import java.nio.file.Paths

trait MultiMapDBSupport extends ObjectStoreSupport {
  this: LightDB =>

  private var map = Map.empty[String, ObjectStore]

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = {
    val store = map.getOrElse(collection.collectionName, MapDBStore(directory.map(_.resolve(collection.collectionName).resolve("store"))))
    synchronized {
      map += collection.collectionName -> store
    }
    store
  }
}
