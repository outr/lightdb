package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.store.StoreManager

@EmbeddedTest
class ChronicleMapSpec extends AbstractKeyValueSpec with ChronicleMapAvailability {
  override def storeManager: StoreManager = ChronicleMapStore
}
