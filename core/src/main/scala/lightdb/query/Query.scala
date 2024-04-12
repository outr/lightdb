package lightdb.query

import cats.effect.IO
import lightdb.index.IndexSupport
import lightdb.Document

case class Query[D <: Document[D]](indexSupport: IndexSupport[D],
                                   filter: Option[Filter[D]] = None,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   pageSize: Int = 1_000,
                                   countTotal: Boolean = false) {
  def filter(filter: Filter[D]): Query[D] = copy(filter = Some(filter))
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def clearSort: Query[D] = copy(sort = Nil)
  def scoreDocs(b: Boolean): Query[D] = copy(scoreDocs = b)
  def pageSize(size: Int): Query[D] = copy(pageSize = size)
  def countTotal(b: Boolean): Query[D] = copy(countTotal = b)

  def search()(implicit context: SearchContext[D]): IO[PagedResults[D]] = indexSupport.doSearch(
    query = this,
    context = context,
    offset = 0,
    after = None
  )
  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, D] = {
    val io = search().map { page1 =>
      fs2.Stream.emit(page1) ++ fs2.Stream.unfoldEval(page1) { page =>
        page.next().map(_.map(p => p -> p))
      }
    }
    fs2.Stream.force(io)
      .flatMap(_.stream)
  }
}