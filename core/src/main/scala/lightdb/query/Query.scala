package lightdb.query

import cats.Eq
import cats.effect.IO
import lightdb.index.{Index, IndexSupport, Materialized}
import lightdb.model.AbstractCollection
import lightdb.spatial.GeoPoint
import lightdb.util.DistanceCalculator
import lightdb.{Document, Id}
import squants.space.Length

case class Query[D <: Document[D], V](indexSupport: IndexSupport[D],
                                      collection: AbstractCollection[D],
                                      convert: D => IO[V],
                                      filter: Option[Filter[D]] = None,
                                      sort: List[Sort] = Nil,
                                      scoreDocs: Boolean = false,
                                      offset: Int = 0,
                                      pageSize: Int = 1_000,
                                      limit: Option[Int] = None,
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
      q = q.filter(indexSupport.distanceFilter(
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

  def search()(implicit context: SearchContext[D]): IO[PagedResults[D, V]] = indexSupport.doSearch(
    query = this,
    context = context,
    offset = offset,
    limit = limit,
    after = None
  )

  def pageStream(implicit context: SearchContext[D]): fs2.Stream[IO, PagedResults[D, V]] = {
    val io = search().map { page1 =>
      fs2.Stream.emit(page1) ++ fs2.Stream.unfoldEval(page1) { page =>
        page.next().map(_.map(p => p -> p))
      }
    }
    fs2.Stream.force(io)
  }

  def docStream(implicit context: SearchContext[D]): fs2.Stream[IO, D] = pageStream.flatMap(_.docStream)

  def idStream(implicit context: SearchContext[D]): fs2.Stream[IO, Id[D]] = pageStream.flatMap(_.idStream)

  def materialized(implicit context: SearchContext[D]): fs2.Stream[IO, Materialized[D]] = pageStream.flatMap(_.materializedStream)

  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, V] = pageStream.flatMap(_.stream)

  def grouped[F](index: Index[F, D],
                 direction: SortDirection = SortDirection.Ascending)
                (implicit context: SearchContext[D]): fs2.Stream[IO, (F, fs2.Chunk[D])] = sort(Sort.ByField(index, direction))
    .docStream
    .groupAdjacentBy(doc => index.get(doc).head)(Eq.fromUniversalEquals)

  object scored {
    def stream(implicit context: SearchContext[D]): fs2.Stream[IO, (V, Double)] = pageStream.flatMap(_.scoredStream)

    def toList: IO[List[(V, Double)]] = indexSupport.withSearchContext { implicit context =>
      stream.compile.toList
    }
  }

  def toIdList: IO[List[Id[D]]] = indexSupport.withSearchContext { implicit context =>
    idStream.compile.toList
  }

  def toList: IO[List[V]] = indexSupport.withSearchContext { implicit context =>
    stream.compile.toList
  }

  def first: IO[Option[V]] = indexSupport.withSearchContext { implicit context =>
    stream.take(1).compile.last
  }

  def one: IO[V] = first.map(_.getOrElse(throw new RuntimeException(s"No results for query: $this")))

  def count: IO[Int] = indexSupport.withSearchContext { implicit context =>
    idStream.compile.count.map(_.toInt)
  }
}