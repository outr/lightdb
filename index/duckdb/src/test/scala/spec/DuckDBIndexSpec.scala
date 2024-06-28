package spec

import lightdb.duckdb.DuckDBIndexer
import lightdb.index.Indexer
import lightdb.store.{AtomicMapStore, StoreManager}

class DuckDBIndexSpec extends AbstractIndexSpec {
  override protected def supportsAggregateFunctions: Boolean = false
  override protected def supportsParsed: Boolean = false

  override protected def storeManager: StoreManager = AtomicMapStore

  override protected def indexer(model: Person.type): Indexer[Person, Person.type] = DuckDBIndexer()
}