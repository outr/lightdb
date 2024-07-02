package lightdb.async

import cats.effect.IO
import fabric.Json
import lightdb.{Field, Filter, Query, SearchResults, Sort, SortDirection, Transaction}
import lightdb.collection.Collection
import lightdb.doc.DocModel
import lightdb.util.GroupedIterator

case class AsyncQuery[Doc, Model <: DocModel[Doc]](collection: Collection[Doc, Model],
                                                   filter: Option[Filter[Doc]] = None,
                                                   sort: List[Sort] = Nil,
                                                   offset: Int = 0,
                                                   limit: Option[Int] = None,
                                                   countTotal: Boolean = false) { query =>
  private def toQuery: Query[Doc, Model] = Query[Doc, Model](collection, filter, sort, offset, limit, countTotal)

  def clearFilters: AsyncQuery[Doc, Model] = copy(filter = None)
  def filter(f: Model => Filter[Doc]): AsyncQuery[Doc, Model] = {
    val filter = f(collection.model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }
  def clearSort: AsyncQuery[Doc, Model] = copy(sort = Nil)
  def sort(sort: Sort*): AsyncQuery[Doc, Model] = copy(sort = this.sort ::: sort.toList)
  def offset(offset: Int): AsyncQuery[Doc, Model] = copy(offset = offset)
  def limit(limit: Int): AsyncQuery[Doc, Model] = copy(limit = Some(limit))
  def clearLimit: AsyncQuery[Doc, Model] = copy(limit = None)
  object search {
    def apply[V](conversion: collection.store.Conversion[V])
                (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, V]] =
      IO.blocking(collection.store.doSearch(
        query = toQuery,
        conversion = conversion
      )).map { searchResults =>
        AsyncSearchResults(
          offset = searchResults.offset,
          limit = searchResults.limit,
          total = searchResults.total,
          stream = fs2.Stream.fromBlockingIterator[IO](searchResults.iterator, 512),
          transaction = transaction
        )
      }

    def docs(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Doc]] = apply(collection.store.Conversion.Doc)
    def value[F](field: Field[Doc, F])
                (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, F]] =
      apply(collection.store.Conversion.Value(field))
    def json(fields: Field[Doc, _]*)(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Json]] =
      apply(collection.store.Conversion.Json(fields.toList))
    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, T]] =
      apply(collection.store.Conversion.Converted(f))
  }

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (F, List[Doc])] = {
    val field = f(collection.model)
    val io = IO.blocking(sort(Sort.ByField(field, direction))
      .toQuery
      .search
      .docs
      .iterator).map { iterator =>
      val grouped = GroupedIterator[Doc, F](iterator, doc => field.get(doc))
      fs2.Stream.fromBlockingIterator[IO](grouped, 512)
    }
    fs2.Stream.force(io)
  }
}