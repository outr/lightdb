package lightdb.document

import lightdb.collection.Collection
import lightdb.transaction.Transaction
import moduload.Priority

trait DocumentListener[D <: Document[D]] {
  def priority: Priority = Priority.Normal

  def init(collection: Collection[D, _]): Unit = ()

  def transactionStart(transaction: Transaction[D]): Unit = ()

  def transactionEnd(transaction: Transaction[D]): Unit = ()

  def preSet(doc: D, transaction: Transaction[D]): Option[D] = Some(doc)

  def postSet(doc: D, `type`: SetType, transaction: Transaction[D]): Unit = ()

  def commit(transaction: Transaction[D]): Unit = ()

  def rollback(transaction: Transaction[D]): Unit = ()

  def preDelete(doc: D, transaction: Transaction[D]): Option[D] = Some(doc)

  def postDelete(doc: D, transaction: Transaction[D]): Unit = ()

  def truncate(transaction: Transaction[D]): Unit = ()

  def dispose(): Unit = ()
}