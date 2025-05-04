package spec
import lightdb.lucene.LuceneStore
import lightdb.store.CollectionManager

@EmbeddedTest
class LuceneFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = LuceneStore
}
