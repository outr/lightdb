package lightdb.store.halo

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.index.{Indexer, SearchResult, SearchResults}
import lightdb.query.Query

case class HaloIndexer[D <: Document[D]](collection: Collection[D]) extends Indexer[D] {
  override def put(value: D): IO[D] = IO.pure(value)
  override def delete(id: Id[D]): IO[Unit] = IO.unit
  override def commit(): IO[Unit] = IO.unit
  override def count(): IO[Int] = collection.store.all().compile.count.map(_.toInt)
  override def search(query: Query[D]): IO[SearchResults[D]] = IO {
    val stream = collection.store
      .all[D]()
      .map { t =>
        collection.fromArray(t.data)
      }
      .filter(query.matches)
      .map { document =>
        new SearchResult[D] {
          override def id: Id[D] = document._id
          override def get(): IO[D] = IO.pure(document)
          override def apply[F](field: Field[D, F]): F = field.getter(document)
        }
      }
    SearchResults(query, -1, stream)
  }
  override def truncate(): IO[Unit] = IO.unit
  override def dispose(): IO[Unit] = IO.unit
}
