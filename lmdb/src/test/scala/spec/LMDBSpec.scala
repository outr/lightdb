package spec

import lightdb.lmdb.LMDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class LMDBSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = LMDBStore
}