package lightdb.lucene

import cats.effect.IO
import lightdb.Document
import lightdb.query.{IndexContext, PagedResults, SearchContext}
import org.apache.lucene.search.ScoreDoc

case class LuceneIndexContext[D <: Document[D]](context: SearchContext[D],
                                                lastScoreDoc: Option[ScoreDoc]) extends IndexContext[D] {
  override def nextPage(currentPage: PagedResults[D]): IO[Option[PagedResults[D]]] = if (currentPage.hasNext) {
    currentPage.query.indexSupport.doSearch(
      query = currentPage.query,
      context = context,
      offset = currentPage.offset + currentPage.query.pageSize,
      after = Some(currentPage)
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}
