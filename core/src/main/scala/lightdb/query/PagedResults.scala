package lightdb.query

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.{Document, Id}

case class PagedResults[D <: Document[D]](query: Query[D],
                                          context: PageContext[D],
                                          offset: Int,
                                          total: Int,
                                          idsAndScores: List[(Id[D], Double)],
                                          getter: Option[Id[D] => IO[D]] = None) {
  lazy val page: Int = offset / query.pageSize
  lazy val pages: Int = math.ceil(total.toDouble / query.pageSize.toDouble).toInt

  lazy val ids: List[Id[D]] = idsAndScores.map(_._1)
  lazy val scores: List[Double] = idsAndScores.map(_._2)

  def idStream: fs2.Stream[IO, Id[D]] = fs2.Stream(ids: _*)

  def idAndScoreStream: fs2.Stream[IO, (Id[D], Double)] = fs2.Stream(idsAndScores: _*)

  def stream: fs2.Stream[IO, D] = idStream
    .evalMap { id =>
      getter match {
        case Some(g) => g(id)
        case None => query.collection(id)
      }
    }

  def scoredStream: fs2.Stream[IO, (D, Double)] = idAndScoreStream
    .evalMap {
      case (id, score) => getter match {
        case Some(g) => g(id).map(doc => doc -> score)
        case None => query.collection(id).map(doc => doc -> score)
      }
    }

  def docs: IO[List[D]] = ids.map(query.collection(_)).sequence

  def hasNext: Boolean = pages > (page + 1)

  def next(): IO[Option[PagedResults[D]]] = context.nextPage(this)
}
