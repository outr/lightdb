package spec
import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: LuceneStore.type = LuceneStore
}
