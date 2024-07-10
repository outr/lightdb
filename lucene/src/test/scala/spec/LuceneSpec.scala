package spec
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

class LuceneSpec extends AbstractBasicSpec {
  override protected def aggregationSupported: Boolean = false

  override def storeManager: StoreManager = LuceneStore
}
