package lightdb.query

import cats.effect.IO
import lightdb.Document

trait IndexContext[D <: Document[D]] {
  def nextPage(currentPage: PagedResults[D]): IO[Option[PagedResults[D]]]
}
