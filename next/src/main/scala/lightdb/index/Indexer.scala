package lightdb.index

import cats.effect.IO
import lightdb.aggregate.AggregateQuery
import lightdb.document.Document
import lightdb.filter.Filter
import lightdb.query.{PagedResults, Query}
import lightdb.spatial.GeoPoint
import lightdb.transaction.Transaction
import squants.space.Length

trait Indexer[D <: Document[D]] {
  def doSearch[V](query: Query[D, V],
                  transaction: Transaction[D],
                  offset: Int,
                  limit: Option[Int],
                  after: Option[PagedResults[D, V]]): IO[PagedResults[D, V]]

  def aggregate(query: AggregateQuery[D])(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]]

  def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, radius: Length): Filter[D] =
    throw new UnsupportedOperationException("Distance filtering is not supported on this indexer")
}