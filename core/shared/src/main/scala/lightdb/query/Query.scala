package lightdb.query

import cats.effect.IO
import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.{SearchResult, SearchResults}

case class Query[D <: Document[D]](collection: Collection[D],
                                   filter: Option[Filter[D]] = None,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   offset: Int = 0,
                                   limit: Int = 1_000_000) {
  // TODO: Pagination support to lower the limit!
  def filter(filter: Filter[D]): Query[D] = copy(filter = Some(filter))
  def clearFilter: Query[D] = copy(filter = None)
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def limit(limit: Int): Query[D] = copy(limit = limit)
  def scoreDocs(b: Boolean = true): Query[D] = copy(scoreDocs = b)
  def search(): IO[SearchResults[D]] = collection.indexer.search(this)
  def stream(): fs2.Stream[IO, SearchResult[D]] = fs2.Stream.force(search().map(_.stream))
  def documentsStream(): fs2.Stream[IO, D] = stream().evalMap(_.get())

  def matches(document: D): Boolean = filter.forall(_.matches(document))
}