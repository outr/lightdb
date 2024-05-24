package lightdb.query

import cats.effect.IO
import lightdb.index.IndexSupport
import lightdb.model.AbstractCollection
import lightdb.{Document, Id}

case class Query[D <: Document[D]](indexSupport: IndexSupport[D],
                                   collection: AbstractCollection[D],
                                   filter: Option[Filter[D]] = None,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   offset: Int = 0,
                                   pageSize: Int = 1_000,
                                   countTotal: Boolean = true) {
  def filter(filter: Filter[D]): Query[D] = copy(filter = Some(filter))
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def clearSort: Query[D] = copy(sort = Nil)
  def scoreDocs(b: Boolean): Query[D] = copy(scoreDocs = b)
  def offset(offset: Int): Query[D] = copy(offset = offset)
  def pageSize(size: Int): Query[D] = copy(pageSize = size)
  def countTotal(b: Boolean): Query[D] = copy(countTotal = b)

  def search()(implicit context: SearchContext[D]): IO[PagedResults[D]] = indexSupport.doSearch(
    query = this,
    context = context,
    offset = offset,
    after = None
  )

  def pageStream(implicit context: SearchContext[D]): fs2.Stream[IO, PagedResults[D]] = {
    val io = search().map { page1 =>
      fs2.Stream.emit(page1) ++ fs2.Stream.unfoldEval(page1) { page =>
        page.next().map(_.map(p => p -> p))
      }
    }
    fs2.Stream.force(io)
  }
  def idStream(implicit context: SearchContext[D]): fs2.Stream[IO, Id[D]] = pageStream.flatMap(_.idStream)
  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, D] = pageStream.flatMap(_.stream)

  object scored {
    def stream(implicit context: SearchContext[D]): fs2.Stream[IO, (D, Double)] = pageStream.flatMap(_.scoredStream)
    def toList: IO[List[(D, Double)]] = indexSupport.withSearchContext { implicit context =>
      stream.compile.toList
    }
  }

  def toIdList: IO[List[Id[D]]] = indexSupport.withSearchContext { implicit context =>
    idStream.compile.toList
  }

  def toList: IO[List[D]] = indexSupport.withSearchContext { implicit context =>
    stream.compile.toList
  }

  def count: IO[Int] = indexSupport.withSearchContext { implicit context =>
    idStream.compile.count.map(_.toInt)
  }
}