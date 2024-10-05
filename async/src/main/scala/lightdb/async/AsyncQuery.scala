package lightdb.async

import cats.effect.IO
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

case class AsyncQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](collection: Collection[Doc, Model],
                                                                         filter: Option[Filter[Doc]] = None,
                                                                         sort: List[Sort] = Nil,
                                                                         offset: Int = 0,
                                                                         limit: Option[Int] = None,
                                                                         countTotal: Boolean = false,
                                                                         scoreDocs: Boolean = false,
                                                                         minDocScore: Option[Double] = None,
                                                                         facets: List[FacetQuery[Doc]] = Nil) { query =>
  private[async] def toQuery: Query[Doc, Model] = Query[Doc, Model](collection, filter, sort, offset, limit, countTotal, scoreDocs, minDocScore, facets)

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
                  (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (V, Double)] = {
        val io = search(conversion)
          .map(_.scoredStream)
        fs2.Stream.force(io)
      }

      def docs(implicit transaction: Transaction[Doc]): fs2.Stream[IO, (Doc, Double)] = apply(Conversion.Doc())

      def value[F](f: Model => Field[Doc, F])
                  (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (F, Double)] =
        apply(Conversion.Value(f(collection.model)))

      def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): fs2.Stream[IO, (Id[Doc], Double)] =
        value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

      def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): fs2.Stream[IO, (Json, Double)] =
        apply(Conversion.Json(f(collection.model)))

      def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): fs2.Stream[IO, (T, Double)] =
        apply(Conversion.Converted(f))

      def materialized(f: Model => List[Field[Doc, _]])
                      (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (MaterializedIndex[Doc, Model], Double)] =
        apply(Conversion.Materialized[Doc, Model](f(collection.model)))

      def indexes()(implicit transaction: Transaction[Doc]): fs2.Stream[IO, (MaterializedIndex[Doc, Model], Double)] = {
        val fields = collection.model.fields.filter(_.indexed)
        apply(Conversion.Materialized[Doc, Model](fields))
      }

      def docAndIndexes()(implicit transaction: Transaction[Doc]): fs2.Stream[IO, (MaterializedAndDoc[Doc, Model], Double)] = {
        apply(Conversion.DocAndIndexes[Doc, Model]())
      }

      def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                             from: Geo.Point,
                             sort: Boolean = true,
                             radius: Option[Distance] = None)
                            (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (DistanceAndDoc[Doc], Double)] =
        apply(Conversion.Distance(f(collection.model), from, sort, radius))
    }

    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): fs2.Stream[IO, V] = {
      val io = search(conversion)
        .map(_.stream)
      fs2.Stream.force(io)
    }

    def docs(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Doc] = apply(Conversion.Doc())

    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): fs2.Stream[IO, F] =
      apply(Conversion.Value(f(collection.model)))

    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): fs2.Stream[IO, Id[Doc]] =
      value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): fs2.Stream[IO, Json] =
      apply(Conversion.Json(f(collection.model)))

    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): fs2.Stream[IO, T] =
      apply(Conversion.Converted(f))

    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): fs2.Stream[IO, MaterializedIndex[Doc, Model]] =
      apply(Conversion.Materialized[Doc, Model](f(collection.model)))

    def indexes()(implicit transaction: Transaction[Doc]): fs2.Stream[IO, MaterializedIndex[Doc, Model]] = {
      val fields = collection.model.fields.filter(_.indexed)
      apply(Conversion.Materialized[Doc, Model](fields))
    }

    def docAndIndexes()(implicit transaction: Transaction[Doc]): fs2.Stream[IO, MaterializedAndDoc[Doc, Model]] = {
      apply(Conversion.DocAndIndexes[Doc, Model]())
    }

    def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                           from: Geo.Point,
                           sort: Boolean = true,
                           radius: Option[Distance] = None)
                          (implicit transaction: Transaction[Doc]): fs2.Stream[IO, DistanceAndDoc[Doc]] =
      apply(Conversion.Distance(f(collection.model), from, sort, radius))
  }

  object search {
    def apply[V](conversion: Conversion[Doc, V])
                (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, V]] =
      IO.blocking(collection.store.doSearch(
        query = toQuery,
        conversion = conversion
      )).map { searchResults =>
        AsyncSearchResults(
          model = collection.model,
          offset = searchResults.offset,
          limit = searchResults.limit,
          total = searchResults.total,
          scoredStream = fs2.Stream.fromBlockingIterator[IO](searchResults.iteratorWithScore, 512),
          facetResults = searchResults.facetResults,
          transaction = transaction
        )
      }

    def docs(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, Doc]] = apply(Conversion.Doc())

    def value[F](f: Model => Field[Doc, F])
                (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, F]] =
      apply(Conversion.Value(f(collection.model)))

    def id(implicit transaction: Transaction[Doc], ev: Model <:< DocumentModel[_]): IO[AsyncSearchResults[Doc, Model, Id[Doc]]] =
      value(m => ev(m)._id.asInstanceOf[UniqueIndex[Doc, Id[Doc]]])

    def json(f: Model => List[Field[Doc, _]])(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, Json]] =
      apply(Conversion.Json(f(collection.model)))

    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, T]] =
      apply(Conversion.Converted(f))

    def materialized(f: Model => List[Field[Doc, _]])
                    (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] =
      apply(Conversion.Materialized(f(collection.model)))

    def indexes()(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, MaterializedIndex[Doc, Model]]] = {
      val fields = collection.model.fields.filter(_.indexed)
      apply(Conversion.Materialized(fields))
    }

    def docAndIndexes()(implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, MaterializedAndDoc[Doc, Model]]] = {
      apply(Conversion.DocAndIndexes())
    }

    def distance[G <: Geo](f: Model => Field[Doc, List[G]],
                           from: Geo.Point,
                           sort: Boolean = true,
                           radius: Option[Distance] = None)
                          (implicit transaction: Transaction[Doc]): IO[AsyncSearchResults[Doc, Model, DistanceAndDoc[Doc]]] =
      apply(Conversion.Distance(f(collection.model), from, sort, radius))
  }

  def toList(implicit transaction: Transaction[Doc]): IO[List[Doc]] = stream.docs.compile.toList

  def first(implicit transaction: Transaction[Doc]): IO[Option[Doc]] = stream.docs.take(1).compile.last

  def one(implicit transaction: Transaction[Doc]): IO[Doc] = stream.docs.take(1).compile.lastOrError

  def count(implicit transaction: Transaction[Doc]): IO[Int] = copy(limit = Some(1), countTotal = true)
    .search.docs.map(_.total.get)

  def aggregate(f: Model => List[AggregateFunction[_, _, Doc]]): AsyncAggregateQuery[Doc, Model] =
    AsyncAggregateQuery(toQuery, f(collection.model))

  def grouped[F](f: Model => Field[Doc, F],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[Doc]): fs2.Stream[IO, (F, List[Doc])] = {
    val field = f(collection.model)
    val state = new IndexingState
    val io = IO.blocking(sort(Sort.ByField(field, direction))
      .toQuery
      .search
      .docs
      .iterator).map { iterator =>
      val grouped = GroupedIterator[Doc, F](iterator, doc => field.get(doc, field, state))
      fs2.Stream.fromBlockingIterator[IO](grouped, 512)
    }
    fs2.Stream.force(io)
  }
}