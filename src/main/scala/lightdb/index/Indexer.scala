package lightdb.index

import cats.effect.IO
import lightdb.{Document, Id}

trait Indexer[D <: Document[D]] {
  def put(value: D): IO[D]

  def delete(id: Id[D]): IO[Unit]

  def flush(): IO[Unit]

  def dispose(): IO[Unit]
}