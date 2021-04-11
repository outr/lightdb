package lightdb.store

import lightdb.Document
import lightdb.collection.Collection

trait ObjectStoreSupport {
  def store[D <: Document[D]](collection: Collection[D]): ObjectStore
}