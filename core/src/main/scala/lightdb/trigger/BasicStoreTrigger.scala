package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.{Collection, Store}
import lightdb.transaction.Transaction
import rapid.Task

import scala.annotation.unchecked.uncheckedVariance

trait BasicStoreTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends StoreTrigger[Doc, Model] {
  def store: Store[Doc, Model @uncheckedVariance]

  protected def adding(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit]

  protected def modifying(oldDoc: Doc, newDoc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit]

  protected def removing(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit]

  override final def insert(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit] = adding(doc)

  override final def upsert(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit] =
    transaction.get(doc._id).map {
      case Some(current) => modifying(current, doc)
      case None => adding(doc)
    }

  override final def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc, Model]): Task[Unit] = {
    transaction(_ => index -> value).flatMap { doc =>
      removing(doc)
    }
  }

  override final def truncate: Task[Unit] = store.transaction { implicit transaction =>
    transaction.stream.map(doc => removing(doc)(transaction)).drain
  }
}
