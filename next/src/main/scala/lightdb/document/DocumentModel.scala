package lightdb.document

import cats.effect.IO
import lightdb.util.Unique
import lightdb.Id
import lightdb.collection.Collection

trait DocumentModel[D <: Document[D]] {
  // TODO: listeners

//  private[lightdb] def init(collection: Collection[D]): IO[Unit] = {
//
//  }

  def id(value: String = Unique()): Id[D] = Id(value)
}