package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id, ObjectMapping}

trait Indexer {
  def put[D <: Document[D]](value: D, mapping: ObjectMapping[D]): IO[D]

  def delete[D <: Document[D]](id: Id[D], mapping: ObjectMapping[D]): IO[Unit]

  def commit[D <: Document[D]](mapping: ObjectMapping[D]): IO[Unit]

  def count(): IO[Long]

  def search[D <: Document[D]](limit: Int = 1000): IO[PagedResults[D]]

  def dispose(): IO[Unit]
}