package lightdb.store.halo

import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.Indexer

trait HaloIndexerSupport {
  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = HaloIndexer(collection)
}
