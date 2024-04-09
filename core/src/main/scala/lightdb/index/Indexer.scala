package lightdb.index

import cats.effect.IO
import lightdb.{Collection, Document, Id}

trait Indexer[D <: Document[D]] {
  def collection: Collection[D]

  private[lightdb] def delete(id: Id[D]): IO[Unit]

  def commit(): IO[Unit]

  def count(): IO[Int]
}
