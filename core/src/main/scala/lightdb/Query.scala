package lightdb

import fabric.Json
import lightdb.field.Field._
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.FacetQuery
import lightdb.field.{Field, IndexingState}
import lightdb.filter._
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.store.{Conversion, Store, StoreMode}
import lightdb.transaction.Transaction
import lightdb.util.GroupedIterator
import rapid.{Forge, Task}

case class Query[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                    store: Store[Doc, Model],
                                                                    filter: Option[Filter[Doc]] = None,
                                                                    sort: List[Sort] = Nil,
                                                                    offset: Int = 0,
                                                                    limit: Option[Int] = None,
                                                                    countTotal: Boolean = false,
                                                                    scoreDocs: Boolean = false,
                                                                    minDocScore: Option[Double] = None,
                                                                    facets: List[FacetQuery[Doc]] = Nil) {
  query =>
  def scored: Query[Doc, Model] = copy(scoreDocs = true)

  def minDocScore(min: Double): Query[Doc, Model] = copy(
    scoreDocs = true,
    minDocScore = Some(min)
  )

  def clearFilters: Query[Doc, Model] = copy(filter = None)

  def filter(f: Model => Filter[Doc]): Query[Doc, Model] = {
    val filter = f(model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }

  def facet(f: Model => FacetField[Doc],
            path: List[String] = Nil,
            childrenLimit: Option[Int] = Some(10),
            dimsLimit: Option[Int] = Some(10)): Query[Doc, Model] = {
    val facetField = f(model)
    val facetQuery = FacetQuery(facetField, path, childrenLimit, dimsLimit)
    copy(facets = facetQuery :: facets)
  }

  def facets(f: Model => List[FacetField[Doc]],
             childrenLimit: Option[Int] = Some(10),
             dimsLimit: Option[Int] = Some(10)): Query[Doc, Model] = {
    val facetFields = f(model)
    val facetQueries = facetFields.map(ff => FacetQuery(ff, Nil, childrenLimit, dimsLimit))
    copy(facets = facets ::: facetQueries)
  }

  def clearSort: Query[Doc, Model] = copy(sort = Nil)

  def sort(sort: Sort*): Query[Doc, Model] = copy(sort = this.sort ::: sort.toList)

  def offset(offset: Int): Query[Doc, Model] = copy(offset = offset)

  def limit(limit: Int): Query[Doc, Model] = copy(limit = Some(limit))

  def clearLimit: Query[Doc, Model] = copy(limit = None)

  def countTotal(b: Boolean): Query[Doc, Model] = copy(countTotal = b)

  object search {
    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = {
      val storeMode = store.storeMode
      if (Query.Validation || (Query.WarnFilteringWithoutIndex && storeMode.isAll)) {
        val notIndexed = filter.toList.flatMap(_.fields(model)).filter(!_.indexed)
        if (storeMode.isIndexes) {
          if (notIndexed.nonEmpty) {
            throw NonIndexedFieldException(query, notIndexed)
          }
        } else {
          if (Query.WarnFilteringWithoutIndex && notIndexed.nonEmpty) {
            scribe.warn(s"Inefficient query filtering on non-indexed field(s): ${notIndexed.map(_.name).mkString(", ")}")
          }
        }
      }
      store.doSearch(
        query = query,
        conversion = conversion
      )
    }

    def docs(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, Doc]] = apply(Conversion.Doc())

    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, F]] =
      apply(Conversion.Value(f(model)))

    def id(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, Id[Doc]]] =
      value(m => m._id)

    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, Json]] =
      apply(Conversion.Json(f(model)))

    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, T]] =
      apply(Conversion.Converted(f))

    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] = {
      val fields = f(model)
      apply(Conversion.Materialized(fields))
    }

    def indexes()(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] = {
      val fields = model.fields.filter(_.indexed)
      apply(Conversion.Materialized(fields))
    }

    def docAndIndexes()(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, MaterializedAndDoc[Doc, Model]]] = {
      apply(Conversion.DocAndIndexes())
    }

    def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                           from: Geo.Point,
                           sort: Boolean = true,
                           radius: Option[Distance] = None)
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, DistanceAndDoc[Doc]]] = {
      val field = f(model)
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

  /**
   * Processes through each result record from the query modifying the data in the database.
   *
   * @param establishLock whether to establish an id lock to avoid concurrent modification (defaults to true)
   * @param deleteOnNone whether to delete the record if the function returns None (defaults to true)
   * @param safeModify whether to use safe modification. This results in loading the same object twice, but should never
   *                   risk concurrent modification occurring. (defaults to true)
   * @param f the processing function for records
   */
  def process(establishLock: Boolean = true,
              deleteOnNone: Boolean = true,
              safeModify: Boolean = true)
             (f: Forge[Doc, Option[Doc]])
             (implicit transaction: Transaction[Doc]): Unit = rapid.Stream.force(search.docs.map(_.stream))
    .evalMap { doc =>
      if (safeModify) {
        store.modify(doc._id, establishLock, deleteOnNone) {
          case Some(doc) => f(doc)
          case None => Task.pure(None)
        }
      } else {
        store.lock(doc._id, Some(doc), establishLock) { current =>
          f(current.getOrElse(doc)).flatMap {
            case Some(modified) => store.upsert(modified).when(!current.contains(modified))
            case None => store.delete(store.idField, doc._id).when(deleteOnNone)
          }.map(_ => None)
        }
      }
    }
    .drain

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream.force(search.docs.map(_.stream))

  def toList(implicit transaction: Transaction[Doc]): Task[List[Doc]] = search.docs.flatMap(_.list)

  def first(implicit transaction: Transaction[Doc]): Task[Doc] = search.docs.flatMap(_.stream.first)

  def firstOption(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = search.docs.flatMap(_.stream.firstOption)

  def count(implicit transaction: Transaction[Doc]): Task[Int] = copy(limit = Some(1), countTotal = true)
    .search.docs.map(_.total.get)

  protected def distanceSearch[G <: Geo](field: Field[Doc, List[G]],
                                         from: Geo.Point,
                                         sort: Boolean,
                                         radius: Option[Distance])
                                        (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, DistanceAndDoc[Doc]]] = {
    search(Conversion.Distance(field, from, sort, radius))
  }

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AggregateQuery[Doc, Model] =
    AggregateQuery(this, f(model))

  // TODO: Support this via stream
  /*def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): GroupedIterator[Doc, F] = {
    val field = f(model)
    val state = new IndexingState
    val iterator = sort(Sort.ByField(field, direction))
      .search
      .docs
      .iterator
    GroupedIterator[Doc, F](iterator, doc => field.get(doc, field, state))
  }*/
}

object Query {
  /**
   * If true, validates queries before execution and errors for runtime validation errors like attempting to filter on
   * a field that is not indexed when StoreMode is Indexed. Defaults to true.
   */
  var Validation: Boolean = true

  /**
   * If true, logs a warning for queries that are using non-indexed fields. Defaults to true.
   */
  var WarnFilteringWithoutIndex: Boolean = true
}