package lightdb.async

import cats.effect.IO
import lightdb.doc.Document
import lightdb.transaction.Transaction

case class AsyncSearchResults[Doc <: Document[Doc], V](offset: Int,
                                      limit: Option[Int],
                                      total: Option[Int],
                                      stream: fs2.Stream[IO, V],
                                      transaction: Transaction[Doc]) {
  def first: IO[Option[V]] = stream.take(1).compile.toList.map(_.headOption)
  def one: IO[V] = first.map {
    case Some(v) => v
    case None => throw new NullPointerException("No results for search")
  }
}
