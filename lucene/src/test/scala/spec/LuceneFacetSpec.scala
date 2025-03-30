package spec
import lightdb.lucene.LuceneStore
import lightdb.store.{CollectionManager, StoreManager}

@EmbeddedTest
class LuceneFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = LuceneStore
}
