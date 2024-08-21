package spec
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

@EmbeddedTest
class LuceneSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: StoreManager = LuceneStore
}
