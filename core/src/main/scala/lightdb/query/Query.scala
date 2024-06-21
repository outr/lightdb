package lightdb.query

import cats.Eq
import cats.effect.IO
import lightdb.Id
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.index.{Index, Indexer, Materialized}
import lightdb.spatial.{DistanceAndDoc, GeoPoint}
import lightdb.transaction.Transaction
import squants.space.Length

case class Query[D <: Document[D], M <: DocumentModel[D]](indexer: Indexer[D, M],
                                                          collection: Collection[D, M],
                                                          filter: Option[Filter[D]] = None,
                                                          sort: List[Sort] = Nil,
                                                          scoreDocs: Boolean = false,
                                                          offset: Int = 0,
                                                          limit: Option[Int] = None,
                                                          materializedIndexes: List[Index[_, D]] = Nil,
                                                          countTotal: Boolean = true) { query =>
  def clearFilters: Query[D, M] = copy(filter = None)
  def filter(f: M => Filter[D]): Query[D, M] = {
    val filter = f(collection.model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }

  def sort(sort: Sort*): Query[D, M] = copy(sort = this.sort ::: sort.toList)

  def clearSort: Query[D, M] = copy(sort = Nil)

  def scoreDocs(b: Boolean): Query[D, M] = copy(scoreDocs = b)

  def offset(offset: Int): Query[D, M] = copy(offset = offset)

  def limit(limit: Int): Query[D, M] = copy(limit = Some(limit))

  def countTotal(b: Boolean): Query[D, M] = copy(countTotal = b)

  object search {
    def apply[V](conversion: indexer.Conversion[V])
                 (implicit transaction: Transaction[D]): IO[SearchResults[D, V]] = indexer.doSearch(
      query = query,
      transaction = transaction,
      conversion = conversion
    )

    def docs(implicit transaction: Transaction[D]): IO[SearchResults[D, D]] = apply[D](indexer.Conversion.Doc)
    def ids(implicit transaction: Transaction[D]): IO[SearchResults[D, Id[D]]] = apply(indexer.Conversion.Id)
    def materialized(f: M => List[Index[_, D]])
                    (implicit transaction: Transaction[D]): IO[SearchResults[D, Materialized[D]]] = {
      val indexes = f(collection.model)
      val notStored = indexes.filter(!_.store).map(_.name)
      if (notStored.nonEmpty) {
        throw new RuntimeException(s"Cannot use non-stored indexes in Materialized: ${notStored.mkString(", ")}")
      }
      apply(indexer.Conversion.Materialized(indexes))
    }
    def distance(f: M => Index[GeoPoint, D],
                 from: GeoPoint,
                 sort: Boolean = true,
                 radius: Option[Length] = None)
                (implicit transaction: Transaction[D]): IO[SearchResults[D, DistanceAndDoc[D]]] = {
      val index = f(collection.model)
      var q = Query.this
      if (sort) {
        q = q.clearSort.sort(Sort.ByDistance(index, from))
      }
      radius.foreach { r =>
        q = q.filter(_ => index.distance(from, r))
      }
      q.distanceSearch(index, from, sort, radius)
    }
  }

  private def distanceSearch(index: Index[GeoPoint, D],
                             from: GeoPoint,
                             sort: Boolean,
                             radius: Option[Length])
                            (implicit transaction: Transaction[D]): IO[SearchResults[D, DistanceAndDoc[D]]] =
    search(indexer.Conversion.Distance(index, from, sort, radius))

  object stream {
    def docs(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = fs2.Stream.force(search.docs.map(_.stream))
    def scoredDocs(implicit transaction: Transaction[D]): fs2.Stream[IO, (D, Double)] = fs2.Stream.force(search.docs.map(_.scoredStream))
    def ids(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream.force(search.ids.map(_.stream))
    def scoredIds(implicit transaction: Transaction[D]): fs2.Stream[IO, (Id[D], Double)] = fs2.Stream.force(search.ids.map(_.scoredStream))
    def materialized(f: M => List[Index[_, D]])(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] =
      fs2.Stream.force(search.materialized(f).map(_.stream))
    def scoredMaterialized(f: M => List[Index[_, D]])
                          (implicit transaction: Transaction[D]): fs2.Stream[IO, (Materialized[D], Double)] =
      fs2.Stream.force(search.materialized(f).map(_.scoredStream))
    def distance(f: M => Index[GeoPoint, D],
                 from: GeoPoint,
                 sort: Boolean = true,
                 radius: Option[Length] = None)
                (implicit transaction: Transaction[D]): fs2.Stream[IO, DistanceAndDoc[D]] =
      fs2.Stream.force(search.distance(f, from, sort, radius).map(_.stream))
    def scoredDistance(f: M => Index[GeoPoint, D],
                 from: GeoPoint,
                 sort: Boolean = true,
                 radius: Option[Length] = None)
                (implicit transaction: Transaction[D]): fs2.Stream[IO, (DistanceAndDoc[D], Double)] =
      fs2.Stream.force(search.distance(f, from, sort, radius).map(_.scoredStream))
  }

  def aggregate(f: M => List[AggregateFunction[_, _, D]]): AggregateQuery[D, M] = AggregateQuery(this, f(collection.model))

  def grouped[F](f: M => Index[F, D],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[D]): fs2.Stream[IO, (F, fs2.Chunk[D])] = {
    val index = f(collection.model)
    sort(Sort.ByIndex(index, direction))
      .stream
      .docs
      .groupAdjacentBy(doc => index.get(doc).head)(Eq.fromUniversalEquals)
  }

  def first(implicit transaction: Transaction[D]): IO[Option[D]] = stream.docs.take(1).compile.last

  def one(implicit transaction: Transaction[D]): IO[D] = first.map(_.getOrElse(throw new RuntimeException(s"No results for query: $this")))

  def count(implicit transaction: Transaction[D]): IO[Int] = stream.ids.compile.count.map(_.toInt)
}