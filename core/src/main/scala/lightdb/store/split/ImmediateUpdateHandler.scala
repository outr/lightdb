package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Collection, Store}
import rapid.Task

/**
 * Applies search updates immediately as a delta occurs. Leads to blocking and slower updates.
 *
 * This is the default operation in a SplitCollectionTransaction.
 */
case class ImmediateUpdateHandler[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
  override def insert(doc: Doc): Task[Unit] = txn.searching.insert(doc).unit
  override def upsert(doc: Doc): Task[Unit] = txn.searching.upsert(doc).unit
  override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] = txn.searching.delete(_ => index -> value).unit
  override def commit: Task[Unit] = txn.searching.commit
  override def rollback: Task[Unit] = txn.searching.rollback
  override def truncate: Task[Unit] = txn.searching.truncate.unit
  override def close: Task[Unit] = Task.unit
}