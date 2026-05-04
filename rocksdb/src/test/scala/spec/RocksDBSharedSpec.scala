package spec

import lightdb.rocksdb.RocksDBSharedStore

import java.nio.file.Path

@EmbeddedTest
class RocksDBSharedSpec extends AbstractKeyValueSpec {
  override lazy val storeManager: RocksDBSharedStore = RocksDBSharedStore(Path.of("db/RocksDBSharedSpec"))

  // No need to override `dispose` — `LightDB.doDispose` now disposes the storeManager
  // automatically when it's a `Disposable` (RocksDBSharedStore extends it).
}
