package lightdb.trigger

import lightdb.field.Field.UniqueIndex
import lightdb.doc.Document
import lightdb.transaction.Transaction

class CollectionTriggers[Doc <: Document[Doc]] extends CollectionTrigger[Doc] {
  private var list = List.empty[CollectionTrigger[Doc]]

  def +=(trigger: CollectionTrigger[Doc]): Unit = synchronized {
    list = trigger :: list
  }

  def -=(trigger: CollectionTrigger[Doc]): Unit = synchronized {
    list = list.filterNot(_ eq trigger)
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit =
    list.foreach(_.insert(doc))

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit =
    list.foreach(_.upsert(doc))

  override def delete[V](index: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Unit =
    list.foreach(_.delete(index, value))

  override def truncate(): Unit =
    list.foreach(_.truncate())

  override def dispose(): Unit =
    list.foreach(_.dispose())
}