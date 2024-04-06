package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.query.Query

case class SearchResults[D <: Document[D]](query: Query[D],
                                           total: Int,
                                           ids: List[Id[D]],
                                           get: Id[D] => IO[D])