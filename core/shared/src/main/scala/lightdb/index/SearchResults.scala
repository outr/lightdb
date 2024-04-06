package lightdb.index

import cats.effect.IO
import cats.implicits.toTraverseOps
import lightdb.{Document, Id}
import lightdb.query.Query

case class SearchResults[D <: Document[D]](query: Query[D],
                                           total: Int,
                                           ids: List[Id[D]],
                                           get: Id[D] => IO[D]) {
  def idStream(): fs2.Stream[IO, Id[D]] = fs2.Stream[IO, Id[D]](ids: _*)
  def stream(): fs2.Stream[IO, D] = idStream().evalMap(get)
  def list(): IO[List[D]] = ids.map(get).sequence
}