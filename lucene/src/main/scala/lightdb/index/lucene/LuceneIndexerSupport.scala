package lightdb.index.lucene

import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.Document

trait LuceneIndexerSupport {
  protected def autoCommit: Boolean = false

  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection, autoCommit)
}