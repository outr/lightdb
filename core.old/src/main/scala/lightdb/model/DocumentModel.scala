package lightdb.model

import cats.effect.IO
import cats.implicits._
import lightdb.index.Index
import lightdb.{Document, Id, IndexedLinks, Unique}

trait DocumentModel[D <: Document[D]] {
  type I[F] = Index[F, D]

  def id(value: String = Unique()): Id[D] = Id(value)

  protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {}

  def reIndex(collection: AbstractCollection[D]): IO[Unit] = IO.unit
}
