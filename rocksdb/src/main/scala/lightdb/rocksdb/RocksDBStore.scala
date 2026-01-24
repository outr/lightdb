package lightdb.rocksdb

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.store._
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.transaction.Transaction
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, DBOptions, FlushOptions, LRUCache, Options, RocksDB}
import profig.Profig
import rapid.Task
import fabric.rw.intRW

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

class RocksDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      private[rocksdb] val rocksDB: RocksDB,
                                                                      sharedStore: Option[RocksDBSharedStoreInstance],
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      lightDB: LightDB,
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) with PrefixScanningStore[Doc, Model] {
  override type TX = RocksDBTransaction[Doc, Model]

  private val iteratorThreads: Int = Profig("lightdb.rocksdb.iteratorThreadPoolSize").opt[Int].getOrElse(16)
  private[rocksdb] val iteratorThreadPool: RocksDBIteratorThreadPool =
    new RocksDBIteratorThreadPool(namePrefix = s"rocksdb-$name", size = iteratorThreads)

  private[rocksdb] var handle: Option[ColumnFamilyHandle] = None
  resetHandle()

  private[rocksdb] def resetHandle(): Unit = handle = sharedStore.map { ss =>
    ss.existingHandle match {
      case Some(handle) => handle
      case None =>
        // Ensure new column families use the same table config (block cache, bloom filters, etc.)
        rocksDB.createColumnFamily(
          new ColumnFamilyDescriptor(ss.handle.getBytes(StandardCharsets.UTF_8), RocksDBStore.sharedColumnFamilyOptions)
        )
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    RocksDBTransaction(this, parent)
  }

  override protected def doDispose(): Task[Unit] =
    super.doDispose()
      .next(iteratorThreadPool.dispose())
      .next(Task {
        val o = new FlushOptions
        handle match {
          case Some(h) =>
            rocksDB.flush(o, h)
          case None =>
            rocksDB.flush(o)
            rocksDB.closeE()
        }
      })
}

object RocksDBStore extends PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = RocksDBStore[Doc, Model]

  // NOTE: For your dedupe workload (massive repeated prefix scans), direct I/O often hurts more than it helps.
  // Disabling it allows Linux page cache + readahead to work in our favor and can dramatically improve scan throughput.
  // If you later want to re-enable, this should become configurable; for now we keep it hardcoded per your preference.
  private val UseDirectIO: Boolean = false

  // Shared across *all* RocksDB instances in this JVM to keep memory bounded even when a MultiStore opens many DBs.
  // This is especially important when using direct reads: without a reasonably sized block cache, read throughput
  // for prefix scans can collapse.
  private lazy val sharedBlockCache: LRUCache = {
    // Off-heap native memory. Scale with heap to keep hot index/data blocks resident and speed up prefix seeks.
    // (This is the dominant cost in your dedupe logs: `keys=...`.)
    val heap = Runtime.getRuntime.maxMemory()
    val bytes: Long =
      if heap >= 48L * 1024L * 1024L * 1024L then 2L * 1024L * 1024L * 1024L // 2 GiB
      else if heap >= 24L * 1024L * 1024L * 1024L then 1L * 1024L * 1024L * 1024L // 1 GiB
      else 512L * 1024L * 1024L // 512 MiB
    new LRUCache(bytes)
  }

  // Keep native options alive for the lifetime of the JVM. Closing these immediately after open can effectively
  // invalidate the underlying native config (and in the worst case can crash in native code).
  private[rocksdb] lazy val sharedTableConfig: BlockBasedTableConfig =
    new BlockBasedTableConfig()
      .setBlockCache(sharedBlockCache)
      .setCacheIndexAndFilterBlocks(true)
      .setPinL0FilterAndIndexBlocksInCache(true)
      .setFilterPolicy(new BloomFilter(10.0, false))

  private[rocksdb] lazy val sharedColumnFamilyOptions: ColumnFamilyOptions =
    new ColumnFamilyOptions()
      .setTableFormatConfig(sharedTableConfig)
      .setOptimizeFiltersForHits(true)

  private lazy val sharedDBOptions: DBOptions =
    new DBOptions()
      .setStatsDumpPeriodSec(0)
      .setCreateIfMissing(true)
      .setIncreaseParallelism(math.max(4, Runtime.getRuntime.availableProcessors() / 2))
      .setMaxSubcompactions(4)
      .setBytesPerSync(1 << 20) // 1 MiB
      .setWalBytesPerSync(1 << 20) // 1 MiB
      .setUseDirectReads(UseDirectIO)
      .setUseDirectIoForFlushAndCompaction(UseDirectIO)
      .setMaxOpenFiles(-1)
      // For read-heavy workloads (like large dedupe traversals), too many background jobs can steal CPU/IO from reads.
      // Keep this conservative and let the application control parallelism.
      .setMaxBackgroundJobs(math.max(2, Runtime.getRuntime.availableProcessors()))

  def createRocksDB(directory: Path): (RocksDB, List[ColumnFamilyHandle]) = {
    RocksDB.loadLibrary()

    Files.createDirectories(directory.getParent)
    val path = directory.toAbsolutePath.toString
    val columnFamilies = new util.ArrayList[ColumnFamilyDescriptor]
    columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, sharedColumnFamilyOptions))

    // Only list existing CFs if the DB already exists. For brand-new DBs, listColumnFamilies can throw.
    if Files.exists(directory) then {
      val listOpts = new Options()
      try {
        RocksDB.listColumnFamilies(listOpts, path)
          .asScala
          .foreach { name =>
            columnFamilies.add(new ColumnFamilyDescriptor(name, sharedColumnFamilyOptions))
          }
      } catch {
        case _: Throwable =>
          // If the DB isn't initialized yet (or listing fails for any reason), just proceed with default CF.
          ()
      } finally {
        listOpts.close()
      }
    }
    val handles = new util.ArrayList[ColumnFamilyHandle]()
    RocksDB.open(sharedDBOptions, path, columnFamilies, handles) -> handles.asScala.toList
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): RocksDBStore[Doc, Model] =
    new RocksDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      rocksDB = createRocksDB(path.get)._1,
      sharedStore = None,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
}
