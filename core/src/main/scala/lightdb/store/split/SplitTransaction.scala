package lightdb.store.split

import lightdb.transaction.Transaction

case class SplitTransaction[Doc](storage: Transaction[Doc], searching: Transaction[Doc]) extends Transaction[Doc] {
  override def commit(): Unit = {
    storage.commit()
    searching.commit()
  }

  override def rollback(): Unit = {
    storage.rollback()
    searching.rollback()
  }
}
