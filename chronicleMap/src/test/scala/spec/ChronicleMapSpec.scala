package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.store.StoreManager

//@EmbeddedTest
class ChronicleMapSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = ChronicleMapStore
}
