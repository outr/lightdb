package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.{Collection, Store}
import lightdb.transaction.RollbackSupport
import rapid.Task

trait SearchUpdateHandler[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
] {
  def txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]

  def insert(doc: Doc): Task[Unit]
  def upsert(doc: Doc): Task[Unit]
  def delete(id: Id[Doc]): Task[Unit]
  def commit: Task[Unit]
  def rollback: Task[Unit]
  def truncate: Task[Unit]
  def close: Task[Unit]
}

object SearchUpdateHandler {
  def rollbackIfSupported(txn: lightdb.transaction.Transaction[?, ?]): Task[Unit] =
    txn match {
      case r: RollbackSupport[?, ?] => r.rollback
      case _ => Task.unit
    }
}