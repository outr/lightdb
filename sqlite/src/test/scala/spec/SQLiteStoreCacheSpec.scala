package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

/** SQL-backend instance of the point-cache suite. Catches the read-through /
  * write-through cache going stale on SQL stores, whose native batch flush
  * (SQLStoreTransaction.flushOps) must track cache writes like the generic
  * Transaction.applyWriteOps path does. */
@EmbeddedTest
class SQLiteStoreCacheSpec extends AbstractStoreCacheSpec {
  override protected def storeManager: CollectionManager = SQLiteStore
}
