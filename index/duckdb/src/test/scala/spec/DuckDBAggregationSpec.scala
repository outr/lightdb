package spec

import lightdb.duckdb.DuckDBIndexer
import lightdb.index.Indexer
import lightdb.store.{AtomicMapStore, StoreManager}

class DuckDBAggregationSpec extends AbstractAggregationSpec {
  override protected def storeManager: StoreManager = AtomicMapStore

  override protected def indexer(model: Person.type): Indexer[Person, Person.type] = DuckDBIndexer()
}
