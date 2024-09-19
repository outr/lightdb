package spec
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

@EmbeddedTest
class LuceneFacetSpec extends AbstractFacetSpec {
  override def storeManager: StoreManager = LuceneStore
}
