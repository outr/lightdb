package lightdb

import fabric.Json
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.doc.{DocModel, DocumentModel}
import lightdb.filter.Filter
import lightdb.materialized.MaterializedIndex
import lightdb.spatial.{DistanceAndDoc, GeoPoint}
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.util.GroupedIterator

case class Query[Doc, Model <: DocModel[Doc]](collection: Collection[Doc, Model],
                                              filter: Option[Filter[Doc]] = None,
                                              sort: List[Sort] = Nil,
                                              offset: Int = 0,
                                              limit: Option[Int] = None,
                                              countTotal: Boolean = false) { query =>
  def clearFilters: Query[Doc, Model] = copy(filter = None)
  def filter(f: Model => Filter[Doc]): Query[Doc, Model] = {
    val filter = f(collection.model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }
  def clearSort: Query[Doc, Model] = copy(sort = Nil)
  def sort(sort: Sort*): Query[Doc, Model] = copy(sort = this.sort ::: sort.toList)
  def offset(offset: Int): Query[Doc, Model] = copy(offset = offset)
  def limit(limit: Int): Query[Doc, Model] = copy(limit = Some(limit))
  def clearLimit: Query[Doc, Model] = copy(limit = None)
  def countTotal(b: Boolean): Query[Doc, Model] = copy(countTotal = b)
  object search {
    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = collection.store.doSearch(
      query = query,
      conversion = conversion
    )

    def docs(implicit transaction: Transaction[Doc]): SearchResults[Doc, Doc] = apply(Conversion.Doc())
    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): SearchResults[Doc, F] =
      apply(Conversion.Value(f(collection.model)))
    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): SearchResults[Doc, Id[Doc]] =
      value(m => ev(m)._id.asInstanceOf[Field.Unique[Doc, Id[Doc]]])
    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): SearchResults[Doc, Json] =
      apply(Conversion.Json(f(collection.model)))
    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): SearchResults[Doc, T] =
      apply(Conversion.Converted(f))
    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): SearchResults[Doc, MaterializedIndex[Doc, Model]] = {
      val fields = f(collection.model)
      apply(Conversion.Materialized(fields))
    }
    def distance(f: Model => Field[Doc, GeoPoint],
                 from: GeoPoint,
                 sort: Boolean = true,
                 radius: Option[Distance] = None)
                (implicit transaction: Transaction[Doc]): SearchResults[Doc, DistanceAndDoc[Doc]] = {
      val field = f(collection.model)
      var q = Query.this
      if (sort) {
        q = q.clearSort.sort(Sort.ByDistance(field, from))
      }
      radius.foreach { r =>
        q = q.filter(_ => field.distance(from, r))
      }
      q.distanceSearch(field, from, sort, radius)
    }
  }

  def toList(implicit transaction: Transaction[Doc]): List[Doc] = search.docs.list

  def first(implicit transaction: Transaction[Doc]): Option[Doc] = search.docs.iterator.nextOption()

  def one(implicit transaction: Transaction[Doc]): Doc = first.getOrElse(throw new NullPointerException("No results"))

  def count(implicit transaction: Transaction[Doc]): Int = copy(limit = Some(1), countTotal = true)
    .search.docs.total.get

  protected def distanceSearch(field: Field[Doc, GeoPoint],
                               from: GeoPoint,
                               sort: Boolean, radius: Option[Distance])
                              (implicit transaction: Transaction[Doc]): SearchResults[Doc, DistanceAndDoc[Doc]] = {
    search(Conversion.Distance(field, from, sort, radius))
  }

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AggregateQuery[Doc, Model] =
    AggregateQuery(this, f(collection.model))

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): GroupedIterator[Doc, F] = {
    val field = f(collection.model)
    val iterator = sort(Sort.ByField(field, direction))
      .search
      .docs
      .iterator
    GroupedIterator[Doc, F](iterator, doc => field.get(doc))
  }
}