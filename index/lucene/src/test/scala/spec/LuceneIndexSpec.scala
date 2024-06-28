package spec
import lightdb.index.Indexer
import lightdb.lucene.LuceneIndexer
import lightdb.store.{AtomicMapStore, StoreManager}

class LuceneIndexSpec extends AbstractIndexAndSpatialSpec {
  override protected def supportsAggregateFunctions: Boolean = false

  override protected def storeManager: StoreManager = AtomicMapStore

  override protected def indexer(model: Person.type): Indexer[Person, Person.type] = LuceneIndexer()
}