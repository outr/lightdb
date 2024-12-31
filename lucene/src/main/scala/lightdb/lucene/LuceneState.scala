package lightdb.lucene

import lightdb.doc.Document
import lightdb.lucene.index.Index
import lightdb.transaction.TransactionFeature
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.search.IndexSearcher
import rapid.Task

case class LuceneState[Doc <: Document[Doc]](index: Index, hasFacets: Boolean) extends TransactionFeature {
  private var oldIndexSearchers = List.empty[IndexSearcher]
  private var oldTaxonomyReaders = List.empty[TaxonomyReader]
  private var _indexSearcher: IndexSearcher = _
  private var _taxonomyReader: TaxonomyReader = _

  def indexSearcher: IndexSearcher = synchronized {
    if (_indexSearcher == null) {
      _indexSearcher = index.createIndexSearcher()
      if (hasFacets) {
        _taxonomyReader = index.createTaxonomyReader()
      }
    }
    _indexSearcher
  }

  private def releaseIndexSearcher(): Unit = synchronized {
    if (_indexSearcher != null) {
      oldIndexSearchers = _indexSearcher :: oldIndexSearchers
      _indexSearcher = null
    }
    if (_taxonomyReader != null) {
      oldTaxonomyReaders = _taxonomyReader :: oldTaxonomyReaders
      _taxonomyReader = null
    }
  }

  def taxonomyReader: TaxonomyReader = _taxonomyReader

  override def commit(): Task[Unit] = Task {
    index.commit()
    releaseIndexSearcher()
  }

  override def rollback(): Task[Unit] = Task {
    index.rollback()
    releaseIndexSearcher()
  }

  override def close(): Task[Unit] = Task {
    commit()
    oldIndexSearchers.foreach(index.releaseIndexSearch)
    oldTaxonomyReaders.foreach(index.releaseTaxonomyReader)
    if (_indexSearcher != null) index.releaseIndexSearch(_indexSearcher)
    if (_taxonomyReader != null) index.releaseTaxonomyReader(_taxonomyReader)
  }
}
