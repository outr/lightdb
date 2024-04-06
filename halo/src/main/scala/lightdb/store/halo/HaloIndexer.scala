package lightdb.store.halo

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.index.{Indexer, SearchResults}
import lightdb.query.Query

case class HaloIndexer[D <: Document[D]](collection: Collection[D]) extends Indexer[D] {
  override def put(value: D): IO[D] = IO.pure(value)
  override def delete(id: Id[D]): IO[Unit] = IO.unit
  override def commit(): IO[Unit] = IO.unit
  override def count(): IO[Int] = collection.store.all().compile.count.map(_.toInt)
  override def search(query: Query[D]): IO[SearchResults[D]] = collection.store
    .all[D]()
    .map { t =>
      collection.fromArray(t.data)
    }
    .filter(query.matches)
    .compile
    .toList
    .map { list =>
      val map = list.map(doc => doc._id -> doc).toMap
      SearchResults[D](query, list.length, list.map(_._id), id => IO(map(id)))
    }
  override def truncate(): IO[Unit] = IO.unit
  override def dispose(): IO[Unit] = IO.unit
}
