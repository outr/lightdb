package lightdb.query

import cats.effect.IO
import lightdb.field.Field
import lightdb.{Document, Id}

trait ResultDoc[D <: Document[D]] {
  def id: Id[D]

  def get(): IO[D]

  def apply[F](field: Field[D, F]): F
}