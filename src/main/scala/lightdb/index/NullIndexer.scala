package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id}

class NullIndexer[D <: Document[D]] extends Indexer[D] {
  override def put(value: D): IO[D] = IO.pure(value)

  override def delete(id: Id[D]): IO[Unit] = IO.unit

  override def flush(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.unit
}