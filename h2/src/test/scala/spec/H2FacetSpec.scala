package spec
import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2FacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = H2Store
}
