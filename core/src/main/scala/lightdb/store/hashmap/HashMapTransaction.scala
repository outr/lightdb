package lightdb.store.hashmap

import fabric.Json
import fabric.rw.Convertible
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.transaction.Transaction
import rapid.Task

case class HashMapTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: HashMapStore[Doc, Model], parent: Option[Transaction[Doc, Model]]) extends Transaction[Doc, Model] {
  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task(store._map.valuesIterator.map(_.json(store.model.rw))))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      store._map.get(value.asInstanceOf[Id[Doc]])
    } else {
      throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    store.synchronized {
      store._map += doc._id -> doc
    }
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = _insert(doc)

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task(store.map.contains(id))

  override protected def _count: Task[Int] = Task(store._map.size)

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    store.synchronized {
      if (index == store.idField) {
        val id = value.asInstanceOf[Id[Doc]]
        val contains = store.map.contains(id)
        store._map -= id
        contains
      } else {
        throw new UnsupportedOperationException(s"MapStore can only get on _id, but ${index.name} was attempted")
      }
    }
  }

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task {
    store.synchronized {
      val size = store.map.size
      store._map = Map.empty
      size
    }
  }
}
