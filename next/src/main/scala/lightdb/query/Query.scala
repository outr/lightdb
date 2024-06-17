package lightdb.query

import cats.Eq
import cats.effect.IO
import lightdb.Id
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.collection.Collection
import lightdb.document.Document
import lightdb.filter.Filter
import lightdb.index.{Index, Indexer, Materialized, SearchConversion}
import lightdb.spatial.{DistanceAndDoc, DistanceCalculator, GeoPoint}
import lightdb.transaction.Transaction
import squants.space.Length

case class Query[D <: Document[D]](indexer: Indexer[D],
                                      collection: Collection[D],
                                      filter: Option[Filter[D]] = None,
                                      sort: List[Sort] = Nil,
                                      scoreDocs: Boolean = false,
                                      offset: Int = 0,
                                      limit: Option[Int] = None,
                                      materializedIndexes: List[Index[_, D]] = Nil,
                                      countTotal: Boolean = true) { query =>
  def filter(filter: Filter[D], and: Boolean = false): Query[D] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: Filter[D]*): Query[D] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(sort: Sort*): Query[D] = copy(sort = this.sort ::: sort.toList)

  /*def distance(field: Index[GeoPoint, D],
               from: GeoPoint,
               sort: Boolean = true,
               radius: Option[Length] = None): Query[D, DistanceAndDoc[D]] = {
    var q = convert(doc => {
      DistanceAndDoc(
        doc = doc,
        distance = DistanceCalculator(from, field.get(doc).head)
      )
    })
    if (sort) {
      q = q.sort(Sort.ByDistance(field, from))
    }
    radius.foreach { r =>
      q = q.filter(indexer.distanceFilter(
        field = field,
        from = from,
        radius = r
      ))
    }
    q
  }*/

  def clearSort: Query[D] = copy(sort = Nil)

  def scoreDocs(b: Boolean): Query[D] = copy(scoreDocs = b)

  def offset(offset: Int): Query[D] = copy(offset = offset)

  def limit(limit: Int): Query[D] = copy(limit = Some(limit))

  def countTotal(b: Boolean): Query[D] = copy(countTotal = b)

  object search {
    def apply[V](conversion: SearchConversion[D, V])
                 (implicit transaction: Transaction[D]): IO[SearchResults[D, V]] = indexer.doSearch(
      query = query,
      transaction = transaction,
      conversion = conversion,
      offset = offset,
      limit = limit
    )

    def apply[V](f: (D, Double) => IO[V])(implicit transaction: Transaction[D]): fs2.Stream[IO, V] =
      fs2.Stream.force(search(SearchConversion.Doc(f)).map(_.stream))

    def docs(implicit transaction: Transaction[D]): IO[SearchResults[D, D]] =
      apply[D](SearchConversion.Doc((doc, _) => IO.pure(doc)))
    def scoredDocs(implicit transaction: Transaction[D]): IO[SearchResults[D, (D, Double)]] =
      apply[(D, Double)](SearchConversion.Doc((doc, score) => IO.pure(doc -> score)))
    def ids(implicit transaction: Transaction[D]): IO[SearchResults[D, Id[D]]] =
      apply[Id[D]](SearchConversion.Id((id, _) => IO.pure(id)))
    def scoredIds(implicit transaction: Transaction[D]): IO[SearchResults[D, (Id[D], Double)]] =
      apply[(Id[D], Double)](SearchConversion.Id((id, score) => IO.pure(id -> score)))
    def materialized(indexes: Index[_, D]*)
                    (implicit transaction: Transaction[D]): IO[SearchResults[D, Materialized[D]]] = apply(
      SearchConversion.Materialized(indexes.toList, (m, _) => IO.pure(m))
    )
    def scoredMaterialized(indexes: Index[_, D]*)
                    (implicit transaction: Transaction[D]): IO[SearchResults[D, (Materialized[D], Double)]] = apply(
      SearchConversion.Materialized(indexes.toList, (m, s) => IO.pure(m -> s))
    )
  }

  object stream {
    def docs(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = fs2.Stream.force(search.docs.map(_.stream))
    def scoredDocs(implicit transaction: Transaction[D]): fs2.Stream[IO, (D, Double)] = fs2.Stream.force(search.scoredDocs.map(_.stream))
    def ids(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream.force(search.ids.map(_.stream))
    def scoredIds(implicit transaction: Transaction[D]): fs2.Stream[IO, (Id[D], Double)] = fs2.Stream.force(search.scoredIds.map(_.stream))
    def materialized(indexes: Index[_, D]*)(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] = {
      fs2.Stream.force(search.materialized(indexes: _*).map(_.stream))
    }
    def scoredMaterialized(indexes: Index[_, D]*)(implicit transaction: Transaction[D]): fs2.Stream[IO, (Materialized[D], Double)] = fs2.Stream.force(search.scoredMaterialized(indexes: _*).map(_.stream))
  }

  object list {
    def docs(implicit transaction: Transaction[D]): IO[List[D]] = stream.docs.compile.toList
    def scoredDocs(implicit transaction: Transaction[D]): IO[List[(D, Double)]] = stream.scoredDocs.compile.toList
    def ids(implicit transaction: Transaction[D]): IO[List[Id[D]]] = stream.ids.compile.toList
    def scoredIds(implicit transaction: Transaction[D]): IO[List[(Id[D], Double)]] = stream.scoredIds.compile.toList
    def materialized(indexes: Index[_, D]*)(implicit transaction: Transaction[D]): IO[List[Materialized[D]]] = stream.materialized(indexes: _*).compile.toList
    def scoredMaterialized(indexes: Index[_, D]*)(implicit transaction: Transaction[D]): IO[List[(Materialized[D], Double)]] = stream.scoredMaterialized(indexes: _*).compile.toList
  }

  def aggregate(functions: AggregateFunction[_, _, D]*): AggregateQuery[D] = AggregateQuery[D](this, functions.toList)

  def grouped[F](index: Index[F, D],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[D]): fs2.Stream[IO, (F, fs2.Chunk[D])] = sort(Sort.ByField(index, direction))
    .stream
    .docs
    .groupAdjacentBy(doc => index.get(doc).head)(Eq.fromUniversalEquals)

  def first(implicit transaction: Transaction[D]): IO[Option[V]] = stream.take(1).compile.last

  def one(implicit transaction: Transaction[D]): IO[V] = first.map(_.getOrElse(throw new RuntimeException(s"No results for query: $this")))

  def count(implicit transaction: Transaction[D]): IO[Int] = stream.ids.compile.count.map(_.toInt)
}