package lightdb.query

import cats.effect.IO
import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.SearchResult

case class Query[D <: Document[D]](collection: Collection[D],
                                   filter: Option[Filter[D]] = None,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   offset: Int = 0,
                                   limit: Int = 100) {
  def filter(filter: Filter[D]): Query[D] = copy(filter = Some(filter))
  def clearFilter: Query[D] = copy(filter = None)
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def limit(limit: Int): Query[D] = copy(limit = limit)
  def scoreDocs(b: Boolean = true): Query[D] = copy(scoreDocs = b)
  def search(): fs2.Stream[IO, SearchResult[D]] = collection.indexer.search(this)

  def matches(document: D): Boolean = filter.forall(_.matches(document))
}