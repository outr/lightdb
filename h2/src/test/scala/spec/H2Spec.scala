package spec
import lightdb.h2.H2Store
import lightdb.store.StoreManager

@EmbeddedTest
class H2Spec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true     // TODO: Remove this when H2 consistently works reloading from disk in concurrent tests

  override def storeManager: StoreManager = H2Store
}
