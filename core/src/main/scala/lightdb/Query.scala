package lightdb

import fabric.Json
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.FacetQuery
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.filter._
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.store.{Conversion, Store}
import lightdb.transaction.Transaction
import rapid.{Forge, Grouped, Task}

case class Query[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](model: Model,
                                                                       store: Store[Doc, Model],
                                                                       conversion: Conversion[Doc, V],
                                                                       filter: Option[Filter[Doc]] = None,
                                                                       sort: List[Sort] = Nil,
                                                                       offset: Int = 0,
                                                                       limit: Int = 100,
                                                                       countTotal: Boolean = false,
                                                                       scoreDocs: Boolean = false,
                                                                       minDocScore: Option[Double] = None,
                                                                       facets: List[FacetQuery[Doc]] = Nil,
                                                                       arbitraryQuery: Option[ArbitraryQuery] = None) { query =>
  private type Q = Query[Doc, Model, V]

  def scored: Q = copy(scoreDocs = true)

  def minDocScore(min: Double): Q = copy(
    scoreDocs = true,
    minDocScore = Some(min)
  )

  def withArbitraryQuery(query: ArbitraryQuery): Q = copy(arbitraryQuery = Some(query))

  def clearFilters: Q = copy(filter = None)

  def filter(f: Model => Filter[Doc]): Q = {
    val filter = f(model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }

  def filterOption(f: Model => Option[Filter[Doc]]): Q = f(model)
    .map(f => filter(_ => f))
    .getOrElse(this)

  def facet(f: Model => FacetField[Doc],
            path: List[String] = Nil,
            childrenLimit: Option[Int] = Some(10),
            dimsLimit: Option[Int] = Some(10)): Q = {
    val facetField = f(model)
    val facetQuery = FacetQuery(facetField, path, childrenLimit, dimsLimit)
    copy(facets = facetQuery :: facets)
  }

  def facets(f: Model => List[FacetField[Doc]],
             childrenLimit: Option[Int] = Some(10),
             dimsLimit: Option[Int] = Some(10)): Q = {
    val facetFields = f(model)
    val facetQueries = facetFields.map(ff => FacetQuery(ff, Nil, childrenLimit, dimsLimit))
    copy(facets = facets ::: facetQueries)
  }

  def clearSort: Q = copy(sort = Nil)

  def sort(sort: Sort*): Q = copy(sort = this.sort ::: sort.toList)

  def offset(offset: Int): Q = copy(offset = offset)

  def limit(limit: Int): Q = copy(limit = limit)

  def countTotal(b: Boolean): Q = copy(countTotal = b)

  def conversion[T](conversion: Conversion[Doc, T]): Query[Doc, Model, T] = {
    var q: Query[Doc, Model, T] = copy[Doc, Model, T](conversion = conversion)
    conversion match {
      case Conversion.Distance(field, from, sort, radius) =>
        if (sort) {
          q = q.clearSort.sort(Sort.ByDistance(field, from))
        }
        radius.foreach { r =>
          q = q.filter(_ => field.distance(from, r))
        }
      case _ => // Ignore others
    }
    q
  }

  def docs: Query[Doc, Model, Doc] = conversion(Conversion.Doc())
  def value[F](f: Model => Field[Doc, F]): Query[Doc, Model, F] = conversion(Conversion.Value(f(model)))
  def id: Query[Doc, Model, Id[Doc]] = value(_._id)
  def json(f: Model => List[Field[Doc, _]] = _ => model.fields): Query[Doc, Model, Json] =
    conversion(Conversion.Json(f(model)))
  def converted[T](f: Doc => T): Query[Doc, Model, T] = conversion(Conversion.Converted(f))
  def materialized(f: Model => List[Field[Doc, _]] = _ => model.indexedFields): Query[Doc, Model, MaterializedIndex[Doc, Model]] =
    conversion(Conversion.Materialized(f(model)))
  def indexes: Query[Doc, Model, MaterializedIndex[Doc, Model]] = materialized()
  def docAndIndexes: Query[Doc, Model, MaterializedAndDoc[Doc, Model]] = conversion(Conversion.DocAndIndexes())
  def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                         from: Geo.Point,
                         sort: Boolean = true,
                         radius: Option[Distance] = None): Query[Doc, Model, DistanceAndDoc[Doc]] =
    conversion(Conversion.Distance(
      field = f(model),
      from = from,
      sort = sort,
      radius = radius
    ))

  def search(implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = {
    if (arbitraryQuery.nonEmpty && !store.supportsArbitraryQuery) {
      throw new UnsupportedOperationException(s"Arbitrary query is set, but not allowed with this store (${store.getClass.getSimpleName})")
    }
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
    store.doSearch(this)
  }

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[V] = rapid.Stream.force(search.map(_.stream))

  def streamScored(implicit transaction: Transaction[Doc]): rapid.Stream[(V, Double)] =
    rapid.Stream.force(search.map(_.streamWithScore))

  /**
   * Processes through each result record from the query modifying the data in the database.
   *
   * @param establishLock whether to establish an id lock to avoid concurrent modification (defaults to true)
   * @param deleteOnNone  whether to delete the record if the function returns None (defaults to true)
   * @param safeModify    whether to use safe modification. This results in loading the same object twice, but should never
   *                      risk concurrent modification occurring. (defaults to true)
   * @param f             the processing function for records
   */
  def process(establishLock: Boolean = true,
              deleteOnNone: Boolean = true,
              safeModify: Boolean = true)
             (f: Forge[Doc, Option[Doc]])
             (implicit transaction: Transaction[Doc]): Unit = docs.stream
    .evalMap { doc =>
      if (safeModify) {
        store.modify(doc._id, establishLock, deleteOnNone) {
          case Some(doc) => f(doc)
          case None => Task.pure(None)
        }
      } else {
        store.lock(doc._id, Task.pure(Some(doc)), establishLock) { current =>
          f(current.getOrElse(doc)).flatMap {
            case Some(modified) => store.upsert(modified).when(!current.contains(modified))
            case None => store.delete(store.idField, doc._id).when(deleteOnNone)
          }.map(_ => None)
        }
      }
    }
    .drain

  def toList(implicit transaction: Transaction[Doc]): Task[List[V]] = stream.toList

  def first(implicit transaction: Transaction[Doc]): Task[V] = limit(1).stream.first

  def firstOption(implicit transaction: Transaction[Doc]): Task[Option[V]] = limit(1).stream.firstOption

  def count(implicit transaction: Transaction[Doc]): Task[Int] = limit(1).countTotal(true).search.map(_.total.get)

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AggregateQuery[Doc, Model] =
    AggregateQuery(this, f(model))

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): rapid.Stream[Grouped[F, Doc]] = {
    val field = f(model)
    val state = new IndexingState
    sort(Sort.ByField(field, direction)).docs.stream.groupSequential(doc => field.get(doc, field, state))
  }
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