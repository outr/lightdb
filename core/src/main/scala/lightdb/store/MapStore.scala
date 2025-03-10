package lightdb.store

import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import rapid.Task

class MapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                  model: Model,
                                                                  val storeMode: StoreMode[Doc, Model],
                                                                  storeManager: StoreManager) extends Store[Doc, Model](name, model, storeManager) { store =>
  private var map = Map.empty[Id[Doc], Doc]

  override protected def initialize(): Task[Unit] = Task.unit

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    store.synchronized {
      map += id(doc) -> doc
    }
    doc
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = insert(doc)

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task(map.contains(id))

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      map.get(value.asInstanceOf[Id[Doc]])
    } else {
      throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V],
                         value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    store.synchronized {
      if (field == idField) {
        val id = value.asInstanceOf[Id[Doc]]
        val contains = map.contains(id)
        map -= id
        contains
      } else {
        throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
      }
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    map.size
  }

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.fromIterator(Task(map.valuesIterator))

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = throw new UnsupportedOperationException("MapStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    store.synchronized {
      val size = map.size
      map = Map.empty
      size
    }
  }

  override protected def doDispose(): Task[Unit] = Task {
    store.synchronized {
      map = Map.empty
    }
  }
}

object MapStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = new MapStore[Doc, Model](name, model, storeMode, this)
}