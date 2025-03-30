package lightdb.store

import fabric.Json
import fabric.rw.Convertible
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
                                                                  db: LightDB,
                                                                  storeManager: StoreManager) extends Store[Doc, Model](name, model, db, storeManager) { store =>
  private var _map = Map.empty[Id[Doc], Doc]

  def map: Map[Id[Doc], Doc] = _map

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    store.synchronized {
      _map += id(doc) -> doc
    }
    doc
  }

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = _insert(doc)

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task(_map.contains(id))

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      _map.get(value.asInstanceOf[Id[Doc]])
    } else {
      throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V],
                         value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    store.synchronized {
      if (field == idField) {
        val id = value.asInstanceOf[Id[Doc]]
        val contains = _map.contains(id)
        _map -= id
        contains
      } else {
        throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${field.name} was attempted")
      }
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    _map.size
  }

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(_map.valuesIterator.map(_.json(model.rw))))

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.fromIterator(Task(_map.valuesIterator))

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = throw new UnsupportedOperationException("MapStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] = throw new UnsupportedOperationException("MapStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    store.synchronized {
      val size = _map.size
      _map = Map.empty
      size
    }
  }

  override protected def doDispose(): Task[Unit] = Task {
    store.synchronized {
      _map = Map.empty
    }
  }
}

object MapStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = MapStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): MapStore[Doc, Model] = new MapStore[Doc, Model](name, model, storeMode, db, this)
}
