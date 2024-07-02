package lightdb.async

import cats.effect.IO
import lightdb.Transaction

case class AsyncSearchResults[Doc, V](offset: Int,
                                      limit: Option[Int],
                                      total: Option[Int],
                                      stream: fs2.Stream[IO, V],
                                      transaction: Transaction[Doc])
