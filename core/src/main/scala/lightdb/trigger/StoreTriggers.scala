package lightdb.trigger

import lightdb.doc.Document
import lightdb.field.Field.UniqueIndex
import lightdb.transaction.Transaction
import rapid._

class StoreTriggers[Doc <: Document[Doc]] extends StoreTrigger[Doc] {
  private var list = List.empty[StoreTrigger[Doc]]

  def +=(trigger: StoreTrigger[Doc]): Unit = synchronized {
    list = trigger :: list
  }

  def -=(trigger: StoreTrigger[Doc]): Unit = synchronized {
    list = list.filterNot(_ eq trigger)
  }

  override def transactionStart(transaction: Transaction[Doc]): Task[Unit] =
    list.map(_.transactionStart(transaction)).tasks.unit

  override def transactionEnd(transaction: Transaction[Doc]): Task[Unit] =
    list.map(_.transactionEnd(transaction)).tasks.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] =
    list.map(_.insert(doc)).tasks.unit

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Unit] =
    list.map(_.upsert(doc)).tasks.unit

  override def delete[V](index: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Unit] =
    list.map(_.delete(index, value)).tasks.unit

  override def truncate(): Task[Unit] =
    list.map(_.truncate()).tasks.unit

  override def dispose(): Task[Unit] =
    list.map(_.dispose()).tasks.unit
}