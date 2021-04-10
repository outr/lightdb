package lightdb.index.lucene

import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.Indexer

trait LuceneIndexerSupport {
  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection)
}