package lightdb

import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.store.Store

trait LightDB {
  private var _collections = List.empty[Collection[_, _]]

  def collections: List[Collection[_, _]] = _collections

  def init(): Unit = {}

  def collection[Doc, Model <: DocModel[Doc]](name: String,
                                              model: Model,
                                              store: Store[Doc, Model],
                                              maxInsertBatch: Int = 1_000_000,
                                              cacheQueries: Boolean = false): Collection[Doc, Model] = {
    val c = Collection[Doc, Model](name, model, store, maxInsertBatch, cacheQueries)
    synchronized {
      _collections = c :: _collections
    }
    c
  }

  def dispose(): Unit = {}
}
