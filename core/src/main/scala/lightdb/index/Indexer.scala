package lightdb.index

import cats.effect.IO
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.filter.Filter
import lightdb.query.{Query, SearchResults}
import lightdb.spatial.GeoPoint
import lightdb.transaction.Transaction
import squants.space.Length

trait Indexer[D <: Document[D], M <: DocumentModel[D]] extends DocumentListener[D] {
  private var _collection: Collection[D, M] = _
  protected def collection: Collection[D, M] = _collection

  override def init(collection: Collection[D, _]): IO[Unit] = super.init(collection).map { _ =>
    this._collection = collection.asInstanceOf[Collection[D, M]]
  }

  def count(implicit transaction: Transaction[D]): IO[Int]

  def doSearch[V](query: Query[D, M],
                  transaction: Transaction[D],
                  conversion: Conversion[V]): IO[SearchResults[D, V]]

  def rebuild()(implicit transaction: Transaction[D]): IO[Unit] = for {
    _ <- truncate(transaction)
    _ <- collection.stream.evalMap { doc =>
      postSet(doc, transaction)
    }.compile.drain
  } yield ()

  def aggregate(query: AggregateQuery[D, M])(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]]

  def distanceFilter(field: Index[GeoPoint, D], from: GeoPoint, radius: Length): Filter[D] =
    throw new UnsupportedOperationException("Distance filtering is not supported on this indexer")

  sealed trait Conversion[V]

  object Conversion {
    case object Id extends Conversion[lightdb.Id[D]]
    case object Doc extends Conversion[D]
    case class Materialized(indexes: List[Index[_, D]]) extends Conversion[lightdb.index.Materialized[D]]
  }
}