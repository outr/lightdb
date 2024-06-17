package lightdb.query

import cats.effect.IO
import lightdb.document.Document
import lightdb.transaction.Transaction

trait PageContext[D <: Document[D]] {
  def transaction: Transaction[D]

  def nextPage[V](currentPage: PagedResults[D, V]): IO[Option[PagedResults[D, V]]] = if (currentPage.hasNext) {
    currentPage.query.indexer.doSearch(
      query = currentPage.query,
      transaction = transaction,
      offset = currentPage.offset + currentPage.query.pageSize,
      limit = currentPage.query.limit,
      after = Some(currentPage)
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}
