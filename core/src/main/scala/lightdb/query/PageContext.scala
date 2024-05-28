package lightdb.query

import cats.effect.IO
import lightdb.Document

trait PageContext[D <: Document[D]] {
  def context: SearchContext[D]

  def nextPage[V](currentPage: PagedResults[D, V]): IO[Option[PagedResults[D, V]]] = if (currentPage.hasNext) {
    currentPage.query.indexSupport.doSearch(
      query = currentPage.query,
      context = context,
      offset = currentPage.offset + currentPage.query.pageSize,
      limit = currentPage.query.limit,
      after = Some(currentPage)
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}
