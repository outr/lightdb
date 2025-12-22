package spec

import lightdb.store.CollectionManager
import lightdb.store.prefix.PrefixScanningStoreManager

@EmbeddedTest
class RocksDBTraversalStoreBasicSpec extends AbstractBasicSpec with TraversalRocksDBWrappedManager {
  override def storeManager: CollectionManager = traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreFacetSpec extends AbstractFacetSpec with TraversalRocksDBWrappedManager {
  override def storeManager: CollectionManager = traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreExistsChildSpec extends AbstractExistsChildSpec with TraversalRocksDBWrappedManager {
  override def storeManager: CollectionManager = traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreSpecialCasesSpec extends AbstractSpecialCasesSpec with TraversalRocksDBWrappedManager {
  override def storeManager: CollectionManager = traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreSpatialSpec extends AbstractSpatialSpec with TraversalRocksDBWrappedManager {
  override def storeManager: CollectionManager = traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreTraversalSpec extends AbstractTraversalSpec with TraversalRocksDBWrappedPrefixManager {
  override def storeManager: PrefixScanningStoreManager = traversalPrefixStoreManager
}

@EmbeddedTest
class RocksDBTraversalStoreDocPipelineSpec extends AbstractTraversalDocPipelineSpec with TraversalRocksDBWrappedManager {
  override def traversalStoreManager: CollectionManager = super.traversalStoreManager
}

@EmbeddedTest
class RocksDBTraversalStorePersistedIndexBuildSpec extends AbstractTraversalPersistedIndexBuildSpec with TraversalRocksDBWrappedManager {
  override def traversalStoreManager: CollectionManager = super.traversalStoreManager
}


