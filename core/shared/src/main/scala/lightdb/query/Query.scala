package lightdb.query

import cats.effect.IO
import lightdb.Document
import lightdb.collection.Collection
import lightdb.index.SearchResult

case class Query[D <: Document[D]](collection: Collection[D],
                                   filters: List[Filter] = Nil,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   offset: Int = 0,
                                   batchSize: Int = 100) {
  def filter(filters: Filter*): Query[D] = copy(filters = this.filters ::: filters.toList)
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def batchSize(limit: Int): Query[D] = copy(batchSize = limit)
  def scoreDocs(b: Boolean = true): Query[D] = copy(scoreDocs = b)
  def search(): fs2.Stream[IO, SearchResult[D]] = collection.indexer.search(this)
}