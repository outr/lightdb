package lighdb.storage.mapdb

import lightdb.collection.Collection
import lightdb.store.{ObjectStore, ObjectStoreSupport}
import lightdb.{Document, LightDB}

trait SharedMapDBSupport extends ObjectStoreSupport {
  this: LightDB =>

  private lazy val shared: MapDBStore = MapDBStore(directory.map(_.resolve("store")))

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = shared
}