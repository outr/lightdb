package spec

import lightdb.lucene.{LuceneDurability, LuceneStore, LuceneStoreManager}
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager

import scala.concurrent.duration.DurationInt

/** The basic suite with the search index's durable commits deferred: every read must see what the transactions before
  * it committed, exactly as with a durable commit per transaction. */
@EmbeddedTest
class RocksDBAndLuceneDeferredSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  // Tie-break ordering is implementation-defined under Lucene strict-insert NRT probing.
  override protected def scoredResultsOrderingSupported: Boolean = false

  override def storeManager: SplitStoreManager[RocksDBStore.type, LuceneStoreManager] =
    SplitStoreManager(RocksDBStore, LuceneStore.withDurability(LuceneDurability.Deferred(250.millis)))
}
