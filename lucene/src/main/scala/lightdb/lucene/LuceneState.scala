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
  // Whether this transaction has changed the index since it last committed or rolled back. A transaction that only
  // read has nothing to commit or roll back: its commit only releases its searcher, and its abort leaves the writer
  // (and the changes other transactions have pending in it) alone.
  @volatile private var dirty = false

  /** The transaction is about to change the index. */
  def markDirty(): Unit = if !dirty then synchronized {
    if !dirty then {
      index.beginChange()
      dirty = true
    }
  }

  def isDirty: Boolean = dirty

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

  private def commitIfDirty(): Unit = synchronized {
    if dirty then {
      index.transactionCommitted()
      dirty = false
    }
  }

  def commit: Task[Unit] = Task {
    commitIfDirty()
    releaseIndexSearcher()
  }

  def rollback: Task[Unit] = Task {
    synchronized {
      if dirty then {
        dirty = false
        index.transactionRolledBack()
      }
    }
    releaseIndexSearcher()
  }

  def close: Task[Unit] = Task {
    commitIfDirty()
    oldIndexSearchers.foreach(index.releaseIndexSearch)
    oldTaxonomyReaders.foreach(index.releaseTaxonomyReader)
    if _indexSearcher != null then index.releaseIndexSearch(_indexSearcher)
    if _taxonomyReader != null then index.releaseTaxonomyReader(_taxonomyReader)
  }
}
