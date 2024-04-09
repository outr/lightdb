package lightdb.index

import cats.effect.IO
import lightdb.query.SearchContext
import lightdb.{Collection, Document, Id, query}

trait Indexer[D <: Document[D]] {
  def indexSupport: IndexSupport[D]

  private[lightdb] def delete(id: Id[D]): IO[Unit]

  def commit(): IO[Unit]

  def count(): IO[Int]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return]
}
