package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import rapid.Task

class DirectWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    doInsert: Doc => Task[Doc],
    doUpsert: Doc => Task[Doc],
    doDelete: Id[Doc] => Task[Boolean]
) extends WriteHandler[Doc, Model] {
  override def write(op: WriteOp[Doc]): Task[Unit] = op match {
    case WriteOp.Insert(doc) => doInsert(doc).unit
    case WriteOp.Upsert(doc) => doUpsert(doc).unit
    case WriteOp.Delete(id) => doDelete(id).unit
  }

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task.pure(None)

  override def flush: Task[Unit] = Task.unit

  override def clear: Task[Unit] = Task.unit

  override def close: Task[Unit] = Task.unit
}
