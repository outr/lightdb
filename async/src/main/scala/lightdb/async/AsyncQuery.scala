package lightdb.async

import cats.effect.IO
import fabric.Json
import lightdb.aggregate.AggregateFunction
import lightdb.{Field, Id, Query, Sort, SortDirection}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.util.GroupedIterator

case class AsyncQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](collection: Collection[Doc, Model],
                                                        filter: Option[Filter[Doc]] = None,
                                                        sort: List[Sort] = Nil,
                                                        offset: Int = 0,
                                                        limit: Option[Int] = None,
                                                        countTotal: Boolean = false) { query =>
  private[async] def toQuery: Query[Doc, Model] = Query[Doc, Model](collection, filter, sort, offset, limit, countTotal)

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
  def countTotal(b: Boolean): AsyncQuery[Doc, Model] = copy(countTotal = b)
  object search {
    def apply[V](conversion: Conversion[Doc, V])
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

    def docs(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Doc]] = apply(Conversion.Doc())
    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, F]] =
      apply(Conversion.Value(f(collection.model)))
    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): IO[AsyncSearchResults[Doc, Id[Doc]]] =
      value(m => ev(m)._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]])
    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Json]] =
      apply(Conversion.Json(f(collection.model)))
    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, T]] =
      apply(Conversion.Converted(f))
  }

  def stream(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Doc] = {
    fs2.Stream.force(search.docs.map(_.stream))
  }

  def toList(implicit transaction: Transaction[Doc]): IO[List[Doc]] = stream.compile.toList

  def first(implicit transaction: Transaction[Doc]): IO[Option[Doc]] = stream.take(1).compile.last

  def one(implicit transaction: Transaction[Doc]): IO[Doc] = stream.take(1).compile.lastOrError

  def count(implicit transaction: Transaction[Doc]): IO[Int] = copy(limit = Some(1), countTotal = true)
    .search.docs.map(_.total.get)

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AsyncAggregateQuery[Doc, Model] =
    AsyncAggregateQuery(toQuery, f(collection.model))

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