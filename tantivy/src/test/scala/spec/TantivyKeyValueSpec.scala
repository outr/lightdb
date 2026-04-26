package spec

import lightdb.store.StoreManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyKeyValueSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = TantivyStore
  override val CreateRecords: Int = 1_000  // Tantivy commit-per-insert is slow; keep MVP modest
}
