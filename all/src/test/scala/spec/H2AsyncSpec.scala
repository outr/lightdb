package spec
import lightdb.h2.H2Store
import lightdb.store.StoreManager

@EmbeddedTest
class H2AsyncSpec extends AbstractAsyncSpec {
  override lazy val storeManager: StoreManager = H2Store
}
