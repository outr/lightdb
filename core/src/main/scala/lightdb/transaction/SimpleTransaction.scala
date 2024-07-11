package lightdb.transaction

import lightdb.doc.Document

case class SimpleTransaction[Doc <: Document[Doc]]() extends Transaction[Doc] {
  override def commit(): Unit = {}

  override def rollback(): Unit = {}
}
