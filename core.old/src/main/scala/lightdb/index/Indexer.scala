package lightdb.index

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentListener, DocumentModel, SetType}
import lightdb.query.{Query, SearchResults}
import lightdb.spatial.{DistanceAndDoc, GeoPoint}
import lightdb.transaction.Transaction
import squants.space.Length

import java.util.concurrent.atomic.AtomicInteger

trait Indexer[D <: Document[D], M <: DocumentModel[D]] extends DocumentListener[D] {
  private var _collection: Collection[D, M] = _
  protected def collection: Collection[D, M] = _collection
  protected lazy val indexes: List[Index[_, D]] = collection.model.asInstanceOf[Indexed[D]].indexes

  private var counterEnabled = false
  private lazy val counter = new AtomicInteger(-1)

  override def init(collection: Collection[D, _]): Unit = {
    super.init(collection)
    this._collection = collection.asInstanceOf[Collection[D, M]]
  }

  override def postSet(doc: D, `type`: SetType, transaction: Transaction[D]): Unit = {
    super.postSet(doc, `type`, transaction)
    if (`type` == SetType.Insert) {
      if (counterEnabled) counter.incrementAndGet()
    }
  }

  override def postDelete(doc: D, transaction: Transaction[D]): Unit = {
    super.postDelete(doc, transaction)
    if (counterEnabled) counter.decrementAndGet()
  }

  final def count(implicit transaction: Transaction[D]): Int = counter.synchronized {
    if (counterEnabled && collection.store.internalCounter) {
      counter.get()
    } else {
      val count = countInternal
      counter.set(count)
      counterEnabled = true
      count
    }
  }

  protected def countInternal(implicit transaction: Transaction[D]): Int

  def doSearch[V](query: Query[D, M],
                  transaction: Transaction[D],
                  conversion: Conversion[V]): SearchResults[D, V]

  def rebuild()(implicit transaction: Transaction[D]): Unit = {
    truncate(transaction)
    collection.iterator.foreach { doc =>
      postSet(doc, SetType.Replace, transaction)
    }
  }

  def maybeRebuild(): Boolean = collection.transaction { implicit transaction =>
    val storeCount = collection.count
    val indexCount = countInternal
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