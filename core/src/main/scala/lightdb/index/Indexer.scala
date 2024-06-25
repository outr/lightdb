package lightdb.index

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentListener, DocumentModel}
import lightdb.filter.Filter
import lightdb.query.{Query, SearchResults}
import lightdb.spatial.{DistanceAndDoc, GeoPoint}
import lightdb.transaction.Transaction
import squants.space.Length

trait Indexer[D <: Document[D], M <: DocumentModel[D]] extends DocumentListener[D] {
  private var _collection: Collection[D, M] = _
  protected def collection: Collection[D, M] = _collection
  protected lazy val indexes: List[Index[_, D]] = collection.model.asInstanceOf[Indexed[D]].indexes

  override def init(collection: Collection[D, _]): Unit = {
    super.init(collection)
    this._collection = collection.asInstanceOf[Collection[D, M]]
  }

  def count(implicit transaction: Transaction[D]): Int

  def doSearch[V](query: Query[D, M],
                  transaction: Transaction[D],
                  conversion: Conversion[V]): SearchResults[D, V]

  def rebuild()(implicit transaction: Transaction[D]): Unit = {
    truncate(transaction)
    collection.iterator.foreach { doc =>
      postSet(doc, transaction)
    }
  }

  def maybeRebuild(): Boolean = collection.transaction { implicit transaction =>
    val storeCount = collection.count
    val indexCount = count
    val shouldRebuild = storeCount != indexCount
    if (shouldRebuild) {
      scribe.warn(s"Index and Store out of sync for ${collection.name} (Store: $storeCount, Index: $indexCount). Rebuilding index...")
      rebuild()
    }
    shouldRebuild
  }

  def aggregate(query: AggregateQuery[D, M])(implicit transaction: Transaction[D]): Iterator[MaterializedAggregate[D, M]]

  sealed trait Conversion[V]

  object Conversion {
    case object Id extends Conversion[lightdb.Id[D]]
    case object Doc extends Conversion[D]
    case class Materialized(indexes: List[Index[_, D]]) extends Conversion[lightdb.index.MaterializedIndex[D, M]]
    case class Distance(index: Index[GeoPoint, D], from: GeoPoint, sort: Boolean, radius: Option[Length]) extends Conversion[DistanceAndDoc[D]]
  }
}