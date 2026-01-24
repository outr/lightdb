package lightdb.lucene

import lightdb.doc.Document
import lightdb.lucene.index.Index
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.search.IndexSearcher
import rapid.Task

case class LuceneState[Doc <: Document[Doc]](index: Index, hasFacets: Boolean) {
  private var oldIndexSearchers = List.empty[IndexSearcher]
  private var oldTaxonomyReaders = List.empty[TaxonomyReader]
  private var _indexSearcher: IndexSearcher = _
  private var _taxonomyReader: TaxonomyReader = _

  def indexSearcher: IndexSearcher = synchronized {
    if _indexSearcher == null then {
      _indexSearcher = index.createIndexSearcher()
      if hasFacets then {
        _taxonomyReader = index.createTaxonomyReader()
      }
    }
    _indexSearcher
  }

  private def releaseIndexSearcher(): Unit = synchronized {
    if _indexSearcher != null then {
      oldIndexSearchers = _indexSearcher :: oldIndexSearchers
      _indexSearcher = null
    }
    if _taxonomyReader != null then {
      oldTaxonomyReaders = _taxonomyReader :: oldTaxonomyReaders
      _taxonomyReader = null
    }
  }

  def taxonomyReader: TaxonomyReader = _taxonomyReader

  def commit: Task[Unit] = Task {
    index.commit()
    releaseIndexSearcher()
  }

  def rollback: Task[Unit] = Task {
    index.rollback()
    releaseIndexSearcher()
  }

  def close: Task[Unit] = Task {
    commit()
    oldIndexSearchers.foreach(index.releaseIndexSearch)
    oldTaxonomyReaders.foreach(index.releaseTaxonomyReader)
    if _indexSearcher != null then index.releaseIndexSearch(_indexSearcher)
    if _taxonomyReader != null then index.releaseTaxonomyReader(_taxonomyReader)
  }
}
