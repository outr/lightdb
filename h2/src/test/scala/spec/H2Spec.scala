package spec
import lightdb.h2.H2Store
import lightdb.store.StoreManager

@EmbeddedTest
class H2Spec extends AbstractBasicSpec {
  override def storeManager: StoreManager = H2Store
}
