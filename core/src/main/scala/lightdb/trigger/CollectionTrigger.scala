package lightdb.trigger

import lightdb.Field
import lightdb.doc.Document
import lightdb.transaction.Transaction

trait CollectionTrigger[Doc <: Document[Doc]] {
  def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {}
  def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {}
  def delete[V](index: Field.UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Unit = {}
  def truncate(): Unit = {}
  def dispose(): Unit = {}
}