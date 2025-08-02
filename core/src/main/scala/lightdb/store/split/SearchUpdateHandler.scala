package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.store.{Collection, Store}
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
  def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit]
  def commit: Task[Unit]
  def rollback: Task[Unit]
  def truncate: Task[Unit]
  def close: Task[Unit]
}