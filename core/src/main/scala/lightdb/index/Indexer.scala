package lightdb.index

import cats.effect.IO
import fabric.rw.RW
import lightdb.aggregate.AggregateQuery
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.filter.Filter
import lightdb.query.{Query, SearchResults}
import lightdb.spatial.GeoPoint
import lightdb.transaction.Transaction
import squants.space.Length

trait Indexer[D <: Document[D], M <: DocumentModel[D]] extends DocumentListener[D] {
  def doSearch[V](query: Query[D, M],
                  transaction: Transaction[D],
                  conversion: Conversion[V]): IO[SearchResults[D, V]]

  def aggregate(query: AggregateQuery[D, M])(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]]

  def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, radius: Length): Filter[D] =
    throw new UnsupportedOperationException("Distance filtering is not supported on this indexer")

  sealed trait Conversion[V]

  object Conversion {
    case object Id extends Conversion[lightdb.Id[D]]
    case object Doc extends Conversion[D]
    case class Materialized(indexes: Index[_, D]*) extends Conversion[lightdb.index.Materialized[D]]
  }
}