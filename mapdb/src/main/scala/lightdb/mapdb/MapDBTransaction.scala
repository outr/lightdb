package lightdb.mapdb

import fabric.Json
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.Transaction
import rapid.Task

import scala.jdk.CollectionConverters._

case class MapDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: MapDBStore[Doc, Model],
                                                                               parent: Option[Transaction[Doc, Model]]) extends Transaction[Doc, Model] {
  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    store.map.values()
      .iterator()
      .asScala
      .map(toJson)
  })

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      Option(store.map.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"MapDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    store.map.put(doc._id.value, toString(doc))
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task {
    store.map.containsKey(id.value)
  }

  override protected def _count: Task[Int] = Task(store.map.size())

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] =
    Task(store.map.remove(value.asInstanceOf[Id[Doc]].value) != null)

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = count.map { size =>
    store.map.clear()
    size
  }
}
