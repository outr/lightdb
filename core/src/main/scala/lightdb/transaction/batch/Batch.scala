package lightdb.transaction.batch

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.Transaction
import lightdb.util.AtomicQueue
import rapid.*

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait Batch[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  protected def transaction: Transaction[Doc, Model]

  def insert(doc: Doc): Task[Doc]
  def upsert(doc: Doc): Task[Doc]
  def delete(id: Id[Doc]): Task[Unit]
  
  def close: Task[Unit] = Transaction.releaseBatch(transaction, this)
}