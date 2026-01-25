package lightdb.halodb

import fabric.Json
import fabric.rw.{Asable, Convertible}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

case class HaloDBTransaction[Doc <: Document [Doc], Model <: DocumentModel[Doc]](store: HaloDBStore[Doc, Model],
                                                                                 instance: HaloDBInstance,
                                                                                 parent: Option[Transaction[Doc, Model]]) extends Transaction[Doc, Model] {
  override def jsonStream: rapid.Stream[Json] = instance.stream

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    if index == store.idField then {
      instance.get(value.asInstanceOf[Id[Doc]]).map(_.map(_.as[Doc](store.model.rw)))
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override protected def _upsert(doc: Doc): Task[Doc] = Task.defer {
    val json = doc.json(store.model.rw)
    instance.put(doc._id, json).map(_ => doc)
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = instance.exists(id)

  override protected def _count: Task[Int] = instance.count

  override protected def _delete(id: Id[Doc]): Task[Boolean] = instance.delete(id).map(_ => true)

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = instance.truncate()
}
