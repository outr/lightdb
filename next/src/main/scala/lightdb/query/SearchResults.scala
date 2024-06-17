package lightdb.query

import cats.effect.IO
import lightdb.document.Document
import lightdb.transaction.Transaction

case class SearchResults[D <: Document[D], V](offset: Int,
                                              limit: Option[Int],
                                              total: Option[Int],
                                              scoredStream: fs2.Stream[IO, (V, Double)],
                                              transaction: Transaction[D]) {
  lazy val stream: fs2.Stream[IO, V] = scoredStream.map(_._1)
}