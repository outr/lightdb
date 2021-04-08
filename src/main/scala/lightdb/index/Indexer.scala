package lightdb.index

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.{Document, Id, ObjectMapping}

trait Indexer[D <: Document[D]] {
  protected def collection: Collection[D]

  def put(value: D): IO[D]

  def delete(id: Id[D]): IO[Unit]

  def commit(): IO[Unit]

  def count(): IO[Long]

  def search(limit: Int = 1000): IO[PagedResults[D]]

  def dispose(): IO[Unit]
}