package spec

import lightdb.h2.H2Indexer
import lightdb.index.Indexer
import lightdb.store.{AtomicMapStore, StoreManager}

class H2IndexSpec extends AbstractIndexSpec {
  override protected def supportsAggregateFunctions: Boolean = false
  override protected def supportsParsed: Boolean = false

  override protected def storeManager: StoreManager = AtomicMapStore

  override protected def indexer(model: Person.type): Indexer[Person, Person.type] = H2Indexer()
}