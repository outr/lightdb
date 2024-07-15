package spec
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

class LuceneSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: StoreManager = LuceneStore
}
