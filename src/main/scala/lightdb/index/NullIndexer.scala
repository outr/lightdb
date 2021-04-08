package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id, ObjectMapping}

class NullIndexer extends Indexer {
  override def put[D <: Document[D]](value: D, mapping: ObjectMapping[D]): IO[D] = IO.pure(value)

  override def delete[D <: Document[D]](id: Id[D], mapping: ObjectMapping[D]): IO[Unit] = IO.unit

  override def commit[D <: Document[D]](mapping: ObjectMapping[D]): IO[Unit] = IO.unit

  override def count(): IO[Long] = IO.pure(0L)

  override def dispose(): IO[Unit] = IO.unit
}