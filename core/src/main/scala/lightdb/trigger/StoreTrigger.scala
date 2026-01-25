package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

trait StoreTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def transactionStart(transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def transactionEnd(transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def insert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def upsert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def delete(id: Id[Doc], transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def truncate: Task[Unit] = Task.unit
  def dispose: Task[Unit] = Task.unit
}