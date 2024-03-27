package lightdb.index

import cats.effect.IO
import fs2.Stream
import lightdb.Document
import lightdb.query.Query

case class SearchResults[D <: Document[D]](query: Query[D],
                                           total: Int,
                                           stream: Stream[IO, SearchResult[D]]) {
  def documentsStream(): fs2.Stream[IO, D] = stream.evalMap(_.get())
}