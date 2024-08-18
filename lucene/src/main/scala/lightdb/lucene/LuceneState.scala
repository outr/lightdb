package lightdb.lucene

import lightdb.doc.Document
import lightdb.lucene.index.Index
import lightdb.transaction.{Transaction, TransactionFeature}
import org.apache.lucene.search.IndexSearcher

case class LuceneState[Doc <: Document[Doc]](index: Index) extends TransactionFeature {
  private var oldIndexSearchers = List.empty[IndexSearcher]
  private var _indexSearcher: IndexSearcher = _

  def indexSearcher: IndexSearcher = synchronized {
    if (_indexSearcher == null) {
      _indexSearcher = index.createIndexSearcher()
    }
    _indexSearcher
  }

  private def releaseIndexSearcher(): Unit = synchronized {
    if (_indexSearcher != null) {
      oldIndexSearchers = _indexSearcher :: oldIndexSearchers
      _indexSearcher = null
    }
  }

  override def commit(): Unit = {
    index.commit()
    releaseIndexSearcher()
  }

  override def rollback(): Unit = {
    index.rollback()
    releaseIndexSearcher()
  }

  override def close(): Unit = {
    commit()
    oldIndexSearchers.foreach(index.releaseIndexSearch)
    if (_indexSearcher != null) index.releaseIndexSearch(_indexSearcher)
  }
}
