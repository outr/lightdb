package lightdb.index

import cats.effect.IO
import lightdb.collection.Collection
import fs2.Stream
import lightdb.query.Query
import lightdb.{Document, Id}

trait Indexer[D <: Document[D]] {
  protected def collection: Collection[D]

  def put(value: D): IO[D]

  def delete(id: Id[D]): IO[Unit]

  def commit(): IO[Unit]

  def count(): IO[Int]

  def search(query: Query[D]): IO[SearchResults[D]]

  def truncate(): IO[Unit]

  def dispose(): IO[Unit]
}