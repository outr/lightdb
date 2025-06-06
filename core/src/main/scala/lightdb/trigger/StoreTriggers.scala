package lightdb.trigger

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.transaction.Transaction
import rapid._

import scala.annotation.unchecked.uncheckedVariance

class StoreTriggers[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends StoreTrigger[Doc, Model] {
  private var list = List.empty[StoreTrigger[Doc, Model @uncheckedVariance]]

  def +=(trigger: StoreTrigger[Doc, Model @uncheckedVariance]): Unit = synchronized {
    list = trigger :: list
  }

  def -=(trigger: StoreTrigger[Doc, Model @uncheckedVariance]): Unit = synchronized {
    list = list.filterNot(_ eq trigger)
  }

  override def transactionStart(transaction: Transaction[Doc, Model]): Task[Unit] =
    list.map(_.transactionStart(transaction)).tasks.unit

  override def transactionEnd(transaction: Transaction[Doc, Model]): Task[Unit] =
    list.map(_.transactionEnd(transaction)).tasks.unit

  override def insert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] =
    list.map(_.insert(doc, transaction)).tasks.unit

  override def upsert(doc: Doc, transaction: Transaction[Doc, Model]): Task[Unit] =
    list.map(_.upsert(doc, transaction)).tasks.unit

  override def delete[V](index: UniqueIndex[Doc, V], value: V, transaction: Transaction[Doc, Model]): Task[Unit] =
    list.map(_.delete(index, value, transaction)).tasks.unit

  override def truncate: Task[Unit] =
    list.map(_.truncate).tasks.unit

  override def dispose: Task[Unit] =
    list.map(_.dispose).tasks.unit
}
