package lightdb.lucene

import lightdb.lucene.index.Index
import lightdb.transaction.Transaction
import org.apache.lucene.search.IndexSearcher

class LuceneTransaction[Doc](index: Index) extends Transaction[Doc] {
  private var _indexSearcher: IndexSearcher = _

  def indexSearcher: IndexSearcher = {
    if (_indexSearcher == null) {
      _indexSearcher = index.createIndexSearcher()
    }
    _indexSearcher
  }

  override def commit(): Unit = index.commit(this)

  override def rollback(): Unit = index.rollback(this)

  def close(): Unit = {
    commit()
    if (_indexSearcher != null) index.releaseIndexSearch(_indexSearcher)
  }
}
