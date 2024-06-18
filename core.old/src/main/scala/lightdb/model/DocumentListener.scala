package lightdb.model

import cats.effect.IO
import lightdb.Document

trait DocumentListener[D <: Document[D], Value] {
  def apply(action: DocumentAction, value: Value, collection: AbstractCollection[D]): IO[Option[Value]]
}