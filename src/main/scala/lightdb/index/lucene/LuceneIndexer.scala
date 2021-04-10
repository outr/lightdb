package lightdb.index.lucene

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.query.{PagedResults, Query}
import lightdb.{Document, Id}

case class LuceneIndexer[D <: Document[D]](collection: Collection[D]) extends Indexer[D] {
  override def put(value: D): IO[D] = ???

  override def delete(id: Id[D]): IO[Unit] = ???

  override def commit(): IO[Unit] = ???

  override def count(): IO[Long] = ???

  override def search(query: Query[D]): IO[PagedResults[D]] = ???

  override def dispose(): IO[Unit] = ???
}