package lightdb.store

import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb._
import lightdb.Field._
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction

class MapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val storeMode: StoreMode) extends Store[Doc, Model] {
  private var map = Map.empty[Id[Doc], Doc]

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = ()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = synchronized {
    map += id(doc) -> doc
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = synchronized {
    map += id(doc) -> doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean = map.contains(id)

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Option[Doc] = {
    if (field == idField) {
      map.get(value.asInstanceOf[Id[Doc]])
    } else {
      throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V],
                         value: V)(implicit transaction: Transaction[Doc]): Boolean = synchronized {
    if (field == idField) {
      val id = value.asInstanceOf[Id[Doc]]
      val contains = map.contains(id)
      map -= id
      contains
    } else {
      throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    map.size
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = map.valuesIterator

  override def doSearch[V](query: Query[Doc, Model],
                           conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V] = throw new UnsupportedOperationException("MapStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Int = synchronized {
    val size = map.size
    map = Map.empty
    size
  }

  override def dispose(): Unit = synchronized {
    map = Map.empty
  }
}

object MapStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] = new MapStore[Doc, Model](storeMode)
}