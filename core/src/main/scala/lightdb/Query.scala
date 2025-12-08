package lightdb

import fabric.Json
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.FacetQuery
import lightdb.field.Field._
import lightdb.field.{Field, FieldAndValue, IndexingState}
import lightdb.filter._
import lightdb.id.Id
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo, Point}
import lightdb.store.{Collection, Conversion}
import lightdb.transaction.CollectionTransaction
import rapid.{Forge, Grouped, Pull, Task}

case class Query[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](transaction: CollectionTransaction[Doc, Model],
                                                                       conversion: Conversion[Doc, V],
                                                                       filter: Option[Filter[Doc]] = None,
                                                                       sort: List[Sort] = Nil,
                                                                       offset: Int = 0,
                                                                       limit: Option[Int] = None,
                                                                       pageSize: Option[Int] = Some(1000),
                                                                       countTotal: Boolean = false,
                                                                       scoreDocs: Boolean = false,
                                                                       minDocScore: Option[Double] = None,
                                                                       facets: List[FacetQuery[Doc]] = Nil,
                                                                       optimize: Boolean = false) { query =>
  def collection: Collection[Doc, Model] = transaction.store
  def model: Model = collection.model

  private type Q = Query[Doc, Model, V]

  private def validateFilters(q: Query[Doc, Model, V]): Task[Unit] = Task {
    val storeMode = q.collection.storeMode
    if (Query.Validation || (Query.WarnFilteringWithoutIndex && storeMode.isAll)) {
      val notIndexed = q.filter.toList.flatMap(_.fields(q.model)).filter(!_.indexed)
      if (storeMode.isIndexes) {
        if (notIndexed.nonEmpty) {
          throw NonIndexedFieldException(q, notIndexed)
        }
      } else {
        if (Query.WarnFilteringWithoutIndex && notIndexed.nonEmpty) {
          scribe.warn(s"Inefficient query filtering on non-indexed field(s): ${notIndexed.map(_.name).mkString(", ")}")
        }
      }
    }
  }

  private def prepared: Task[Q] = for {
    resolved <- FilterPlanner.resolve(filter, model)
    optimizedFilter = if (optimize) {
      resolved.map(QueryOptimizer.optimize)
    } else {
      resolved
    }
    q = copy(filter = optimizedFilter)
    _ <- validateFilters(q)
  } yield q

  def scored: Q = copy(scoreDocs = true)

  def minDocScore(min: Double): Q = copy(
    scoreDocs = true,
    minDocScore = Some(min)
  )

  def optimized: Q = copy(optimize = true)
  def unOptimized: Q = copy(optimize = false)

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

  def limit(limit: Int): Q = copy(limit = Some(limit))

  def clearLimit: Q = copy(limit = None)

  def clearPageSize: Q = copy(pageSize = None)

  def pageSize(size: Int): Q = copy(pageSize = Some(size))

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
                         from: Point,
                         sort: Boolean = true,
                         radius: Option[Distance] = None): Query[Doc, Model, DistanceAndDoc[Doc]] =
    conversion(Conversion.Distance(
      field = f(model),
      from = from,
      sort = sort,
      radius = radius
    ))

  def search: Task[SearchResults[Doc, Model, V]] = {
    prepared.flatMap(transaction.doSearch(_))
  }

  /**
   * Uses the query to find and delete all the documents that match the criteria in this collection.
   */
  def delete: Task[Int] = prepared.flatMap(transaction.doDelete(_))

  /**
   * Updates all the records that match the criteria in this collection with the fields and values.
   */
  def update(f: => Model => List[FieldAndValue[Doc, _]]): Task[Int] =
    prepared.flatMap(q => transaction.doUpdate(q, f(model)))

  def streamPage: rapid.Stream[V] = rapid.Stream.force(search.map(_.stream))

  def streamScoredPage: rapid.Stream[(V, Double)] =
    rapid.Stream.force(search.map(_.streamWithScore))

  def stream: rapid.Stream[V] = streamScored.map(_._1)

  def streamScored: rapid.Stream[(V, Double)] = {
    if (query.pageSize.nonEmpty) {
      rapid.Stream.merge {
        Task.defer {
          copy(limit = Some(1), countTotal = true).search.map(_.total.get).map { total =>
            val end = limit match {
              case Some(l) => math.min(l, total)
              case None => total
            }
            val pages = query.offset to end by query.pageSize.get
            val iterator = pages.iterator.map { offset =>
              rapid.Stream.force(copy(offset = offset).search.map(_.streamWithScore))
            }
            Pull.fromIterator(iterator)
          }
        }
      }
    } else {      // No pageSize defined...one stream
      rapid.Stream.force(search.map(_.streamWithScore))
    }
  }

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
             (f: Forge[Doc, Option[Doc]]): Unit = docs.stream
    .evalMap { doc =>
      if (safeModify) {
        transaction.modify(doc._id, establishLock, deleteOnNone) {
          case Some(doc) => f(doc)
          case None => Task.pure(None)
        }
      } else {
        collection.lock(doc._id, Task.pure(Some(doc)), establishLock) { current =>
          f(current.getOrElse(doc)).flatMap {
            case Some(modified) => transaction.upsert(modified).when(!current.contains(modified))
            case None => transaction.delete(doc._id).when(deleteOnNone)
          }.map(_ => None)
        }
      }
    }
    .drain

  def toList: Task[List[V]] = stream.toList

  def first: Task[V] = limit(1).stream.first

  def firstOption: Task[Option[V]] = limit(1).stream.firstOption

  def count: Task[Int] = limit(1).countTotal(true).search.map(_.total.get)

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AggregateQuery[Doc, Model] =
    AggregateQuery(this, f(model))

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending): rapid.Stream[Grouped[F, Doc]] = {
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
