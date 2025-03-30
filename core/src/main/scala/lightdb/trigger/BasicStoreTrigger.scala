package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.Task

trait BasicStoreTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends StoreTrigger[Doc] {
  def store: Store[Doc, Model]

  protected def adding(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  protected def modifying(oldDoc: Doc, newDoc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  protected def removing(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit]

  override final def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = adding(doc)

  override final def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] =
    store.get(doc._id).map {
      case Some(current) => modifying(current, doc)
      case None => adding(doc)
    }

  override final def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Unit] = {
    store.query.filter(_ => index === value).docs.stream.foreach(removing).drain
  }

  override final def truncate(): Task[Unit] = store.transaction { implicit transaction =>
    store.stream.foreach(removing).drain
  }
}
