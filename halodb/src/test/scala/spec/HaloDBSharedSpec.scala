package spec

import lightdb.halodb.HaloDBSharedStore
import lightdb.store.StoreManager

import java.nio.file.Path

@EmbeddedTest
class HaloDBSharedSpec extends AbstractKeyValueSpec {
  override lazy val storeManager: StoreManager = HaloDBSharedStore(Path.of("db/HaloDBSharedSpec"), useNameAsPrefix = true)
}