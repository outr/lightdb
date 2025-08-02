package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Collection, Store}
import rapid.Task

/**
 * Ignores deltas causing search to get further and further out-of-sync with storage.
 *
 * This can be useful when many updates need to occur across multiple transactions and then reIndex is called when
 * finished.
 */
case class DisabledUpdateHandler[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
  override def insert(doc: Doc): Task[Unit] = Task.unit
  override def upsert(doc: Doc): Task[Unit] = Task.unit
  override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] = Task.unit
  override def commit: Task[Unit] = Task.unit
  override def rollback: Task[Unit] = Task.unit
  override def truncate: Task[Unit] = Task.unit
  override def close: Task[Unit] = Task.unit
}