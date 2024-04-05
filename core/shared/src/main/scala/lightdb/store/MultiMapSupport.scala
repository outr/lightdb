package lightdb.store
import lightdb.{Document, LightDB}
import lightdb.collection.Collection

trait MultiMapSupport extends ObjectStoreSupport {
  this: LightDB =>

  private var map = Map.empty[String, ObjectStore]

  override def store[D <: Document[D]](collection: Collection[D]): ObjectStore = {
    val store = map.getOrElse(collection.collectionName, new MapStore)
    synchronized {
      map += collection.collectionName -> store
    }
    store
  }
}
