package lightdb.document

import cats.effect.IO
import lightdb.collection.Collection

trait DocumentListener[D <: Document[D]] {
  def init(collection: Collection[D]): IO[Unit] = IO.unit

  // TODO: Replicate listeners
}