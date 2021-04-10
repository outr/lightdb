package lightdb.index

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.query.Query
import lightdb.{Document, Id}

case class NullIndexer[D <: Document[D]](collection: Collection[D]) extends Indexer[D] {
  override def put(value: D): IO[D] = IO.pure(value)

  override def delete(id: Id[D]): IO[Unit] = IO.unit

  override def commit(): IO[Unit] = IO.unit

  override def count(): IO[Long] = IO.pure(0L)

  override def search(query: Query[D]): IO[PagedResults[D]] = ???

  override def dispose(): IO[Unit] = IO.unit
}