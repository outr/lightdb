package lightdb.index

import cats.effect.IO
import fabric.rw.RW
import lightdb.aggregate.AggregateQuery
import lightdb.document.Document
import lightdb.filter.Filter
import lightdb.query.{Query, SearchResults}
import lightdb.spatial.GeoPoint
import lightdb.transaction.Transaction
import squants.space.Length

trait Indexer[D <: Document[D]] {
  def apply[F](name: String,
               get: D => List[F],
               store: Boolean = false,
               sorted: Boolean = false,
               tokenized: Boolean = false)
              (implicit rw: RW[F]): Index[F, D]

  def opt[F](name: String,
             get: D => Option[F],
             store: Boolean = false,
             sorted: Boolean = false,
             tokenized: Boolean = false)
            (implicit rw: RW[F]): Index[F, D] = apply[F](name, doc => get(doc).toList, store, sorted, tokenized)

  def one[F](name: String,
             get: D => F,
             store: Boolean = false,
             sorted: Boolean = false,
             tokenized: Boolean = false)
            (implicit rw: RW[F]): Index[F, D] = apply[F](name, doc => List(get(doc)), store, sorted, tokenized)

  def doSearch[V](query: Query[D],
                  transaction: Transaction[D],
                  conversion: SearchConversion[D, V],
                  offset: Int,
                  limit: Option[Int]): IO[SearchResults[D, V]]

  def aggregate(query: AggregateQuery[D])(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]]

  def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, radius: Length): Filter[D] =
    throw new UnsupportedOperationException("Distance filtering is not supported on this indexer")
}

sealed trait SearchConversion[D <: Document[D], V]

object SearchConversion {
  case class Id[D <: Document[D], V](f: (lightdb.Id[D], Double) => IO[V]) extends SearchConversion[D, V]
  case class Doc[D <: Document[D], V](f: (D, Double) => IO[V]) extends SearchConversion[D, V]
  case class Materialized[D <: Document[D], V](indexes: List[Index[_, D]], f: (lightdb.index.Materialized[D], Double) => IO[V]) extends SearchConversion[D, V]
}