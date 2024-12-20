package lightdb.async

import fabric.Json
import lightdb.aggregate.AggregateFunction
import lightdb._
import lightdb.field.Field._
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetQuery
import lightdb.field.{Field, IndexingState}
import lightdb.filter._
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.store.Conversion
import lightdb.transaction.Transaction
import lightdb.util.GroupedIterator
import rapid.Task

case class AsyncQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](asyncCollection: AsyncCollection[Doc, Model],
                                                                         filter: Option[Filter[Doc]] = None,
                                                                         sort: List[Sort] = Nil,
                                                                         offset: Int = 0,
                                                                         limit: Option[Int] = None,
                                                                         countTotal: Boolean = false,
                                                                         scoreDocs: Boolean = false,
                                                                         minDocScore: Option[Double] = None,
                                                                         facets: List[FacetQuery[Doc]] = Nil) { query =>
  protected def collection: Collection[Doc, Model] = asyncCollection.underlying

  def toQuery: Query[Doc, Model] = Query[Doc, Model](asyncCollection.underlying.model, asyncCollection.underlying.store, filter, sort, offset, limit, countTotal, scoreDocs, minDocScore, facets)

  def scored: AsyncQuery[Doc, Model] = copy(scoreDocs = true)

  def minDocScore(min: Double): AsyncQuery[Doc, Model] = copy(
    scoreDocs = true,
    minDocScore = Some(min)
  )

  def clearFilters: AsyncQuery[Doc, Model] = copy(filter = None)

  def filter(f: Model => Filter[Doc]): AsyncQuery[Doc, Model] = {
    val filter = f(collection.model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }

  def facet(f: Model => FacetField[Doc],
            path: List[String] = Nil,
            childrenLimit: Option[Int] = Some(10),
            dimsLimit: Option[Int] = Some(10)): AsyncQuery[Doc, Model] = {
    val facetField = f(collection.model)
    val facetQuery = FacetQuery(facetField, path, childrenLimit, dimsLimit)
    copy(facets = facetQuery :: facets)
  }

  def facets(f: Model => List[FacetField[Doc]],
             childrenLimit: Option[Int] = Some(10),
             dimsLimit: Option[Int] = Some(10)): AsyncQuery[Doc, Model] = {
    val facetFields = f(collection.model)
    val facetQueries = facetFields.map(ff => FacetQuery(ff, Nil, childrenLimit, dimsLimit))
    copy(facets = facets ::: facetQueries)
  }

  def clearSort: AsyncQuery[Doc, Model] = copy(sort = Nil)

  def sort(sort: Sort*): AsyncQuery[Doc, Model] = copy(sort = this.sort ::: sort.toList)

  def offset(offset: Int): AsyncQuery[Doc, Model] = copy(offset = offset)

  def limit(limit: Int): AsyncQuery[Doc, Model] = copy(limit = Some(limit))

  def clearLimit: AsyncQuery[Doc, Model] = copy(limit = None)

  def countTotal(b: Boolean): AsyncQuery[Doc, Model] = copy(countTotal = b)

  object stream {
    object scored {
      def apply[V](conversion: Conversion[Doc, V])
                  (implicit transaction: Transaction[Doc]): rapid.Stream[(V, Double)] = {
        val io = search(conversion)
          .map(_.scoredStream)
        rapid.Stream.force(io)
      }

      def docs(implicit transaction: Transaction[Doc]): rapid.Stream[(Doc, Double)] = apply(Conversion.Doc())

      def value[F](f: Model => Field[Doc, F])
                  (implicit transaction: Transaction[Doc]): rapid.Stream[(F, Double)] =
        apply(Conversion.Value(f(collection.model)))

      def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): rapid.Stream[(Id[Doc], Double)] =
        value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

      def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): rapid.Stream[(Json, Double)] =
        apply(Conversion.Json(f(collection.model)))

      def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): rapid.Stream[(T, Double)] =
        apply(Conversion.Converted(f))

      def materialized(f: Model => List[Field[Doc, _]])
                      (implicit transaction: Transaction[Doc]): rapid.Stream[(MaterializedIndex[Doc, Model], Double)] =
        apply(Conversion.Materialized[Doc, Model](f(collection.model)))

      def indexes()(implicit transaction: Transaction[Doc]): rapid.Stream[(MaterializedIndex[Doc, Model], Double)] = {
        val fields = collection.model.fields.filter(_.indexed)
        apply(Conversion.Materialized[Doc, Model](fields))
      }

      def docAndIndexes()(implicit transaction: Transaction[Doc]): rapid.Stream[(MaterializedAndDoc[Doc, Model], Double)] = {
        apply(Conversion.DocAndIndexes[Doc, Model]())
      }

      def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                             from: Geo.Point,
                             sort: Boolean = true,
                             radius: Option[Distance] = None)
                            (implicit transaction: Transaction[Doc]): rapid.Stream[(DistanceAndDoc[Doc], Double)] =
        apply(Conversion.Distance(f(collection.model), from, sort, radius))
    }

    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): rapid.Stream[V] = {
      val task = search(conversion)
        .map(_.stream)
      rapid.Stream.force(task)
    }

    def docs(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = apply(Conversion.Doc())

    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): rapid.Stream[F] =
      apply(Conversion.Value(f(collection.model)))

    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): rapid.Stream[Id[Doc]] =
      value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): rapid.Stream[Json] =
      apply(Conversion.Json(f(collection.model)))

    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): rapid.Stream[T] =
      apply(Conversion.Converted(f))

    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedIndex[Doc, Model]] =
      apply(Conversion.Materialized[Doc, Model](f(collection.model)))

    def indexes()(implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedIndex[Doc, Model]] = {
      val fields = collection.model.fields.filter(_.indexed)
      apply(Conversion.Materialized[Doc, Model](fields))
    }

    def docAndIndexes()(implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAndDoc[Doc, Model]] = {
      apply(Conversion.DocAndIndexes[Doc, Model]())
    }

    def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                           from: Geo.Point,
                           sort: Boolean = true,
                           radius: Option[Distance] = None)
                          (implicit transaction: Transaction[Doc]): rapid.Stream[DistanceAndDoc[Doc]] =
      apply(Conversion.Distance(f(collection.model), from, sort, radius))
  }

  object search {
    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, V]] =
      Task(collection.store.doSearch(
        query = toQuery,
        conversion = conversion
      )).map { searchResults =>
        AsyncSearchResults(
          model = collection.model,
          offset = searchResults.offset,
          limit = searchResults.limit,
          total = searchResults.total,
          scoredStream = rapid.Stream.fromIterator(Task(searchResults.iteratorWithScore)),
          facetResults = searchResults.facetResults,
          transaction = transaction
        )
      }

    def docs(implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, Doc]] = apply(Conversion.Doc())

    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, F]] =
      apply(Conversion.Value(f(collection.model)))

    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): Task[AsyncSearchResults[Doc, Model, Id[Doc]]] =
      value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, Json]] =
      apply(Conversion.Json(f(collection.model)))

    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, T]] =
      apply(Conversion.Converted(f))

    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] =
      apply(Conversion.Materialized(f(collection.model)))

    def indexes()(implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] = {
      val fields = collection.model.fields.filter(_.indexed)
      apply(Conversion.Materialized(fields))
    }

    def docAndIndexes()(implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, MaterializedAndDoc[Doc, Model]]] = {
      apply(Conversion.DocAndIndexes())
    }

    def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                           from: Geo.Point,
                           sort: Boolean = true,
                           radius: Option[Distance] = None)
                          (implicit transaction: Transaction[Doc]): Task[AsyncSearchResults[Doc, Model, DistanceAndDoc[Doc]]] =
      apply(Conversion.Distance(f(collection.model), from, sort, radius))
  }

  /**
   * Processes through each result record from the query modifying the data in the database.
   *
   * @param establishLock whether to establish an id lock to avoid concurrent modification (defaults to true)
   * @param deleteOnNone whether to delete the record if the function returns None (defaults to true)
   * @param safeModify whether to use safe modification. This results in loading the same object twice, but should never
   *                   risk concurrent modification occurring. (defaults to true)
   * @param maxConcurrent the number of concurrent threads to process with (defaults to 1 for single-threaded)
   * @param f the processing function for records
   */
  def process(establishLock: Boolean = true,
              deleteOnNone: Boolean = true,
              safeModify: Boolean = true,
              maxConcurrent: Int = 1)
             (f: Doc => Task[Option[Doc]])
             (implicit transaction: Transaction[Doc]): Task[Int] = stream
    .docs
    .par(maxThreads = maxConcurrent) { doc =>
      if (safeModify) {
        asyncCollection.modify(doc._id, establishLock, deleteOnNone) {
          case Some(doc) => f(doc)
          case None => Task.pure(None)
        }
      } else {
        asyncCollection.withLock(doc._id, Task.pure(Some(doc)), establishLock) { current =>
          val io: Task[Option[Doc]] = current match {
            case Some(doc) => f(doc)
            case None => Task.pure(None)
          }
          io.flatTap {
            case Some(modified) if !current.contains(modified) => asyncCollection.upsert(modified)
            case None if deleteOnNone => asyncCollection.delete(doc._id)
            case _ => Task.unit
          }
        }
      }
    }
    .count

  def toList(implicit transaction: Transaction[Doc]): Task[List[Doc]] = stream.docs.toList

  def first(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = stream.docs.take(1).lastOption

  def one(implicit transaction: Transaction[Doc]): Task[Doc] = stream.docs.take(1).last

  def count(implicit transaction: Transaction[Doc]): Task[Int] = copy(limit = Some(1), countTotal = true)
    .search.docs.map(_.total.get)

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AsyncAggregateQuery[Doc, Model] =
    AsyncAggregateQuery(toQuery, f(collection.model))

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): rapid.Stream[(F, List[Doc])] = {
    val field = f(collection.model)
    val state = new IndexingState
    val io = Task(sort(Sort.ByField(field, direction))
      .toQuery
      .search
      .docs
      .iterator).map { iterator =>
      val grouped = GroupedIterator[Doc, F](iterator, doc => field.get(doc, field, state))
      rapid.Stream.fromIterator[(F, List[Doc])](Task(grouped))
    }
    rapid.Stream.force(io)
  }
}