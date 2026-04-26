package benchmark.jmh.complete

import lightdb.LightDB
import lightdb.store.{Collection, CollectionManager, Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

/** Common interface for the benchmark `LightDB` wrapper. Every backend exposes a single store
 *  named `"bench"`; subclasses fix the typing to either `Store` (Kv) or `Collection` (Collection
 *  + Split). Benchmarks that need query support cast through the `asCollection` accessor.
 */
trait BenchDb {
  def db: LightDB
  def store: Store[BenchDoc, BenchDoc.type]

  /** Upcast for collection-only benchmarks. Returns `None` for pure KV backends so callers can
   *  skip cleanly.
   */
  def asCollection: Option[Collection[BenchDoc, BenchDoc.type]] = store match {
    case c: Collection[BenchDoc, BenchDoc.type] @unchecked => Some(c)
    case _ => None
  }
}

object BenchDb {
  def kv[SM <: StoreManager](sm: SM, dir: Option[Path]): BenchDb = new KvDb(sm, dir)
  def collection[SM <: CollectionManager](sm: SM, dir: Option[Path]): BenchDb = new CollDb(sm, dir)

  private final class KvDb[SM0 <: StoreManager](sm: SM0, override val db: KvLightDB[SM0]) extends BenchDb {
    def this(sm: SM0, dir: Option[Path]) = this(sm, new KvLightDB(sm, dir))
    override def store: Store[BenchDoc, BenchDoc.type] = db.bench
  }

  private final class CollDb[SM0 <: CollectionManager](sm: SM0, override val db: CollLightDB[SM0]) extends BenchDb {
    def this(sm: SM0, dir: Option[Path]) = this(sm, new CollLightDB(sm, dir))
    override def store: Collection[BenchDoc, BenchDoc.type] = db.bench
  }
}

private[complete] class KvLightDB[SM0 <: StoreManager](sm: SM0, dir: Option[Path]) extends LightDB {
  override type SM = SM0
  override val storeManager: SM0 = sm
  override val directory: Option[Path] = dir
  override def upgrades: List[DatabaseUpgrade] = Nil
  val bench: storeManager.S[BenchDoc, BenchDoc.type] = store(BenchDoc).withName("bench")()
}

private[complete] class CollLightDB[SM0 <: CollectionManager](sm: SM0, dir: Option[Path]) extends LightDB {
  override type SM = SM0
  override val storeManager: SM0 = sm
  override val directory: Option[Path] = dir
  override def upgrades: List[DatabaseUpgrade] = Nil
  val bench: storeManager.S[BenchDoc, BenchDoc.type] = store(BenchDoc).withName("bench")()
}
