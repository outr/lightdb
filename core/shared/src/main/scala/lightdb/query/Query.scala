package lightdb.query

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.collection.Collection
import lightdb.index.SearchResults

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
  def idStream(): fs2.Stream[IO, Id[D]] = fs2.Stream.force(search()
    .map { searchResults =>
      fs2.Stream[IO, Id[D]](searchResults.ids: _*)
    })
  def stream(): fs2.Stream[IO, D] = fs2.Stream.force(search()
    .map { searchResults =>
      fs2.Stream[IO, Id[D]](searchResults.ids: _*).evalMap(searchResults.get)
    })

  def matches(document: D): Boolean = filter.forall(_.matches(document))
}