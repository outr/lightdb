package lightdb.transaction.batch

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

case class DirectBatch[Doc <: Document[Doc], Model <: DocumentModel[Doc]](transaction: Transaction[Doc, Model]) extends Batch[Doc, Model] {
  override def insert(doc: Doc): Task[Doc] = transaction.insert(doc)
  override def upsert(doc: Doc): Task[Doc] = transaction.upsert(doc)
  override def delete(id: Id[Doc]): Task[Unit] = transaction.delete(id).unit
  override def flush: Task[Int] = Task.pure(0)
}
