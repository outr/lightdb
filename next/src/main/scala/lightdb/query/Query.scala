package lightdb.query

import cats.Eq
import cats.effect.IO
import lightdb.Id
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.collection.Collection
import lightdb.document.Document
import lightdb.filter.Filter
import lightdb.index.{Index, Indexer, Materialized}
import lightdb.spatial.{DistanceAndDoc, DistanceCalculator, GeoPoint}
import lightdb.transaction.Transaction
import squants.space.Length

case class Query[D <: Document[D], V](indexer: Indexer[D],
                                      collection: Collection[D],
                                      convert: D => IO[V],
                                      filter: Option[Filter[D]] = None,
                                      sort: List[Sort] = Nil,
                                      scoreDocs: Boolean = false,
                                      offset: Int = 0,
                                      pageSize: Int = 1_000,
                                      limit: Option[Int] = None,
                                      materializedIndexes: List[Index[_, D]] = Nil,
                                      countTotal: Boolean = true) {
  def evalConvert[T](converter: D => IO[T]): Query[D, T] = copy(convert = converter)
  def convert[T](converter: D => T): Query[D, T] = copy(convert = doc => IO.blocking(converter(doc)))

  def filter(filter: Filter[D], and: Boolean = false): Query[D, V] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: Filter[D]*): Query[D, V] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(sort: Sort*): Query[D, V] = copy(sort = this.sort ::: sort.toList)

  def distance(field: Index[GeoPoint, D],
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
  }

  def clearSort: Query[D, V] = copy(sort = Nil)

  def scoreDocs(b: Boolean): Query[D, V] = copy(scoreDocs = b)

  def offset(offset: Int): Query[D, V] = copy(offset = offset)

  def pageSize(size: Int): Query[D, V] = copy(pageSize = size)

  def limit(limit: Int): Query[D, V] = copy(limit = Some(limit))

  def countTotal(b: Boolean): Query[D, V] = copy(countTotal = b)

  def search()(implicit transaction: Transaction[D]): IO[PagedResults[D, V]] = indexer.doSearch(
    query = this,
    transaction = transaction,
    offset = offset,
    limit = limit,
    after = None
  )

  def pageStream(implicit transaction: Transaction[D]): fs2.Stream[IO, PagedResults[D, V]] = {
    val io = search().map { page1 =>
      fs2.Stream.emit(page1) ++ fs2.Stream.unfoldEval(page1) { page =>
        page.next().map(_.map(p => p -> p))
      }
    }
    fs2.Stream.force(io)
  }

  def docStream(implicit transaction: Transaction[D]): fs2.Stream[IO, D] = pageStream.flatMap(_.docStream)

  def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = pageStream.flatMap(_.idStream)

  def materialized(indexes: Index[_, D]*)
                  (implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] = {
    copy(materializedIndexes = indexes.toList).pageStream.flatMap(_.materializedStream)
  }

  def aggregate(functions: AggregateFunction[_, _, D]*): AggregateQuery[D] = AggregateQuery[D](this, functions.toList)

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, V] = pageStream.flatMap(_.stream)

  def grouped[F](index: Index[F, D],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit transaction: Transaction[D]): fs2.Stream[IO, (F, fs2.Chunk[D])] = sort(Sort.ByField(index, direction))
    .docStream
    .groupAdjacentBy(doc => index.get(doc).head)(Eq.fromUniversalEquals)

  object scored {
    def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, (V, Double)] = pageStream.flatMap(_.scoredStream)

    def toList(implicit transaction: Transaction[D]): IO[List[(V, Double)]] = stream.compile.toList
  }

  def toIdList(implicit transaction: Transaction[D]): IO[List[Id[D]]] = idStream.compile.toList

  def toList(implicit transaction: Transaction[D]): IO[List[V]] = stream.compile.toList

  def first(implicit transaction: Transaction[D]): IO[Option[V]] = stream.take(1).compile.last

  def one(implicit transaction: Transaction[D]): IO[V] = first.map(_.getOrElse(throw new RuntimeException(s"No results for query: $this")))

  def count(implicit transaction: Transaction[D]): IO[Int] = idStream.compile.count.map(_.toInt)
}