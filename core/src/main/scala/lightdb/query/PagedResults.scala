package lightdb.query

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.index.SearchContext
import lightdb.{Document, Id}
import org.apache.lucene.search.ScoreDoc

case class PagedResults[D <: Document[D]](query: Query[D],
                                          context: SearchContext[D],
                                          offset: Int,
                                          total: Int,
                                          ids: List[Id[D]],
                                          private val lastScoreDoc: Option[ScoreDoc]) {
  lazy val page: Int = offset / query.pageSize
  lazy val pages: Int = math.ceil(total.toDouble / query.pageSize.toDouble).toInt

  def stream: fs2.Stream[IO, D] = fs2.Stream(ids: _*)
    .evalMap(id => query.collection(id))

  def docs: IO[List[D]] = ids.map(id => query.collection(id)).sequence

  def hasNext: Boolean = pages > (page + 1)

  def next(): IO[Option[PagedResults[D]]] = if (hasNext) {
    query.doSearch(
      context = context,
      offset = offset + query.pageSize,
      after = lastScoreDoc
    ).map(Some.apply)
  } else {
    IO.pure(None)
  }
}
