package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store
import lightdb.transaction.Transaction
import rapid.Task

import scala.annotation.unchecked.uncheckedVariance

trait BasicStoreTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends StoreTrigger[Doc, Model] {
  def store: Store[Doc, Model @uncheckedVariance]

  protected def adding(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit]

  protected def modifying(oldDoc: Doc, newDoc: Doc, transaction: Transaction[Doc, Model]): Task[Unit]

  protected def removing(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit]

  override final def insert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] = adding(doc, transaction)

  override final def upsert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] =
    transaction.get(doc._id).map {
      case Some(current) => modifying(current, doc, transaction)
      case None => adding(doc, transaction)
    }

  override final def truncate: Task[Unit] = store.transaction { transaction =>
    transaction.stream.map(doc => removing(doc, transaction)).drain
  }
}
