package lightdb.trigger

import lightdb.doc.Document
import lightdb.field.Field
import lightdb.transaction.Transaction
import rapid.Task

trait StoreTrigger[Doc <: Document[Doc]] {
  def transactionStart(transaction: Transaction[Doc]): Task[Unit] = Task.unit
  def transactionEnd(transaction: Transaction[Doc]): Task[Unit] = Task.unit
  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = Task.unit
  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] = Task.unit
  def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Unit] = Task.unit
  def truncate(): Task[Unit] = Task.unit
  def dispose(): Task[Unit] = Task.unit
}