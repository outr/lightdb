package spec
import lightdb.index.Indexer
import lightdb.lucene.LuceneIndexer
import lightdb.store.{AtomicMapStore, StoreManager}

class LuceneIndexSpec extends AbstractIndexSpec {
  override protected def storeManager: StoreManager = AtomicMapStore

  override protected lazy val indexer: Indexer[Person] = LuceneIndexer(Person)
}