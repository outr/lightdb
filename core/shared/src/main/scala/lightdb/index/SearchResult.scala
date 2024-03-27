package lightdb.index

import cats.effect.IO
import lightdb.field.Field
import lightdb.query.Query
import lightdb.{Document, Id}

trait SearchResult[D <: Document[D]] {
  def id: Id[D]

  def get(): IO[D]
  def apply[F](field: Field[D, F]): F
}