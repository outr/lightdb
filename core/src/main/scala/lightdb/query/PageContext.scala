package lightdb.query

import cats.effect.IO
import lightdb.Document

trait PageContext[D <: Document[D]] {
  def context: SearchContext[D]

  def nextPage(currentPage: PagedResults[D]): IO[Option[PagedResults[D]]] = if (currentPage.hasNext) {
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
