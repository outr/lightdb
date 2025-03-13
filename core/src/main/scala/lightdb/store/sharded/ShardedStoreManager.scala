package lightdb.store.sharded

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.sharded.manager.{HashBasedShardManager, ShardManager}
import lightdb.store.{Store, StoreManager, StoreMode}

/**
 * A StoreManager that creates ShardedStore instances, which distribute data across multiple shards.
 *
 * @param storeManager The StoreManager to use for creating the individual shard stores
 * @param shardCount The number of shards to create
 */
case class ShardedStoreManager(storeManager: StoreManager,
                               shardCount: Int,
                               shardManager: ShardManager = HashBasedShardManager) extends StoreManager {
  override lazy val name: String = s"Sharded($storeManager, $shardCount)"

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    // Create N stores representing each shard
    val shards = (0 until shardCount).map { shardIndex =>
      val shardName = s"$name.shard$shardIndex"
      storeManager.create[Doc, Model](db, model, shardName, storeMode)
    }.toVector

    new ShardedStore(
      name = name,
      model = model,
      shardManager = shardManager.create[Doc, Model](model, shards),
      storeMode = storeMode,
      storeManager = this
    )
  }
}