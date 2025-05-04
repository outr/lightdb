package spec

import lightdb.rocksdb.RocksDBSharedStore
import rapid.Task

import java.nio.file.Path

//@EmbeddedTest
class RocksDBSharedSpec extends AbstractKeyValueSpec {
  override lazy val storeManager: RocksDBSharedStore = RocksDBSharedStore(Path.of("db/RocksDBSharedSpec"))

  override def dispose(): Task[Unit] = storeManager.dispose()
}
