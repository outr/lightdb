package lightdb.query

import cats.effect.IO
import lightdb.Document
import lightdb.collection.Collection

case class Query[D <: Document[D]](collection: Collection[D],
                                   filters: List[Filter] = Nil,
                                   sort: List[Sort] = Nil,
                                   scoreDocs: Boolean = false,
                                   offset: Int = 0,
                                   limit: Int = 1000) {
  def filter(filters: Filter*): Query[D] = copy(filters = this.filters ::: filters.toList)
  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)
  def scoreDocs(b: Boolean = true): Query[D] = copy(scoreDocs = b)
  def search(): IO[PagedResults[D]] = collection.indexer.search(this)
}