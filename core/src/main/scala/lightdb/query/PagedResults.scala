package lightdb.query

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.{Document, Id}

case class PagedResults[D <: Document[D]](query: Query[D],
                                          context: IndexContext[D],
                                          offset: Int,
                                          total: Int,
                                          ids: List[Id[D]]) {
  lazy val page: Int = offset / query.pageSize
  lazy val pages: Int = math.ceil(total.toDouble / query.pageSize.toDouble).toInt

  def stream: fs2.Stream[IO, D] = fs2.Stream(ids: _*)
    .evalMap(id => query.indexSupport(id))

  def docs: IO[List[D]] = ids.map(id => query.indexSupport(id)).sequence

  def hasNext: Boolean = pages > (page + 1)

  def next(): IO[Option[PagedResults[D]]] = context.nextPage(this)
}