package lightdb.lucene

import lightdb.doc.Document
import lightdb.lucene.index.Index
import lightdb.transaction.{Transaction, TransactionFeature}
import org.apache.lucene.search.IndexSearcher

case class LuceneState[Doc <: Document[Doc]](index: Index) extends TransactionFeature {
  private var _indexSearcher: IndexSearcher = _

  def indexSearcher: IndexSearcher = {
    if (_indexSearcher == null) {
      _indexSearcher = index.createIndexSearcher()
    }
    _indexSearcher
  }

  override def commit(): Unit = index.commit()

  override def rollback(): Unit = index.rollback()

  override def close(): Unit = {
    commit()
    if (_indexSearcher != null) index.releaseIndexSearch(_indexSearcher)
  }
}
