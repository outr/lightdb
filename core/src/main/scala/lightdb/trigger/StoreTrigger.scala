package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.Transaction
import rapid.Task

trait StoreTrigger[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def transactionStart(transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def transactionEnd(transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def insert(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def upsert(doc: Doc)(implicit transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc, Model]): Task[Unit] = Task.unit
  def truncate: Task[Unit] = Task.unit
  def dispose: Task[Unit] = Task.unit
}