package lightdb.index.lucene

import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.Indexer

trait LuceneIndexerSupport {
  protected def autoCommit: Boolean = false

  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D] = LuceneIndexer(collection, autoCommit)
}