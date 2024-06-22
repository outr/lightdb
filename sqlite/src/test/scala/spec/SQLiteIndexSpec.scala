package spec

import lightdb.index.Indexer
import lightdb.sqlite.SQLiteIndexer
import lightdb.store.{AtomicMapStore, StoreManager}

class SQLiteIndexSpec extends AbstractIndexSpec {
  override protected def supportsAggregateFunctions: Boolean = false

  override protected def storeManager: StoreManager = AtomicMapStore

  override protected def indexer(model: Person.type): Indexer[Person, Person.type] = SQLiteIndexer()
}