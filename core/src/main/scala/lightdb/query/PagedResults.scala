package lightdb.query

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.{Document, Id}

case class PagedResults[D <: Document[D], V](query: Query[D, V],
                                             context: PageContext[D],
                                             offset: Int,
                                             total: Int,
                                             idsAndScores: List[(Id[D], Double)],
                                             getter: Option[Id[D] => IO[D]] = None) {
  lazy val page: Int = offset / query.pageSize
  lazy val pages: Int = math.ceil(query.limit.getOrElse(total).toDouble / query.pageSize.toDouble).toInt

  lazy val ids: List[Id[D]] = idsAndScores.map(_._1)
  lazy val scores: List[Double] = idsAndScores.map(_._2)

  def docs: IO[List[D]] = ids.map(query.collection(_)).sequence

  def values: IO[List[V]] = docs.flatMap(_.map(query.convert).sequence)

  def idStream: fs2.Stream[IO, Id[D]] = fs2.Stream(ids: _*)

  def idAndScoreStream: fs2.Stream[IO, (Id[D], Double)] = fs2.Stream(idsAndScores: _*)

  def docStream: fs2.Stream[IO, D] = idStream
    .evalMap { id =>
      getter match {
        case Some(g) => g(id)
        case None => query.collection(id)
      }
    }

  def stream: fs2.Stream[IO, V] = docStream.evalMap(query.convert)

  def scoredStream: fs2.Stream[IO, (V, Double)] = idAndScoreStream
    .evalMap {
      case (id, score) => getter match {
        case Some(g) => g(id).map(doc => doc -> score)
        case None => query.collection(id).map(doc => doc -> score)
      }
    }
    .evalMap {
      case (doc, score) => query.convert(doc).map(v => v -> score)
    }

  def hasNext: Boolean = pages > (page + 1)

  def next(): IO[Option[PagedResults[D, V]]] = context.nextPage(this)
}
