package lightdb.store.split

import lightdb.doc.Document
import lightdb.transaction.Transaction

case class SplitTransaction[Doc <: Document[Doc]](storage: Transaction[Doc], searching: Transaction[Doc]) extends Transaction[Doc] {
  override def commit(): Unit = {
    storage.commit()
    searching.commit()
  }

  override def rollback(): Unit = {
    storage.rollback()
    searching.rollback()
  }

  override def close(): Unit = {
    super.close()
    storage.close()
    searching.close()
  }
}
