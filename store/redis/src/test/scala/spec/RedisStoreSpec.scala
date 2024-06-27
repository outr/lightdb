package spec

import lightdb.redis.RedisStore
import lightdb.store.StoreManager

class RedisStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = RedisStore
}
