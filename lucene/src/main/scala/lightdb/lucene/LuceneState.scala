package lightdb.lucene

import lightdb.doc.Document
import lightdb.lucene.index.Index
import lightdb.transaction.{Transaction, TransactionFeature}
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.search.IndexSearcher

case class LuceneState[Doc <: Document[Doc]](index: Index) extends TransactionFeature {
  private var oldIndexSearchers = List.empty[IndexSearcher]
  private var oldTaxonomyReaders = List.empty[TaxonomyReader]
  private var _indexSearcher: IndexSearcher = _
  private var _taxonomyReader: TaxonomyReader = _

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

  def taxonomyReader: TaxonomyReader = synchronized {
    if (_taxonomyReader == null) {
      _taxonomyReader = index.createTaxonomyReader()
    }
    _taxonomyReader
  }

  private def releaseTaxonomyReader(): Unit = synchronized {
    if (_taxonomyReader != null) {
      oldTaxonomyReaders = _taxonomyReader :: oldTaxonomyReaders
      _taxonomyReader = null
    }
  }

  override def commit(): Unit = {
    index.commit()
    releaseIndexSearcher()
    releaseTaxonomyReader()
  }

  override def rollback(): Unit = {
    index.rollback()
    releaseIndexSearcher()
    releaseTaxonomyReader()
  }

  override def close(): Unit = {
    commit()
    oldIndexSearchers.foreach(index.releaseIndexSearch)
    oldTaxonomyReaders.foreach(index.releaseTaxonomyReader)
    if (_indexSearcher != null) index.releaseIndexSearch(_indexSearcher)
    if (_taxonomyReader != null) index.releaseTaxonomyReader(_taxonomyReader)
  }
}
