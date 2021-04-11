package lightdb.index.lucene

import com.outr.lucene4s.query.SearchResult
import lightdb.Document
import lightdb.query.{PagedResults, Query, ResultDoc}

case class LucenePagedResults[D <: Document[D]](indexer: LuceneIndexer[D],
                                                query: Query[D],
                                                lpr: com.outr.lucene4s.query.PagedResults[SearchResult]) extends PagedResults[D] {
  override def total: Long = lpr.total

  override def documents: List[ResultDoc[D]] = lpr.entries.toList.map(r => LuceneResultDoc(this, r))
}