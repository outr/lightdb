package lightdb.lucene

import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.*
import lightdb.lucene.index.Index
import lightdb.store.*
import lightdb.store.nested.NestedQueryStore
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.index.{DirectoryReader, SegmentReader}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import rapid.*

import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LuceneStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     lightDB: LightDB,
                                                                     storeManager: StoreManager,
                                                                     val durability: LuceneDurability = LuceneDurability.configured)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager)
    with NestedQueryStore[Doc, Model] {
  override type TX = LuceneTransaction[Doc, Model]
  override def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Native

  override def defaultBatchConfig: BatchConfig = BatchConfig.Direct

  IndexSearcher.setMaxClauseCount(10_000_000)

  /** Whether this store may defer its durable commits: it must be able to rebuild its index from the storage it
    * mirrors, from documents written through its transactions alone. */
  protected def supportsDeferredDurability: Boolean = true

  lazy val index: Index = Index(path, durability match {
    case d: LuceneDurability.Deferred if storeMode.isIndexes && path.nonEmpty && supportsDeferredDurability => d
    case _ => LuceneDurability.Immediate
  })

  lazy val facetsConfig: FacetsConfig = {
    val c = new FacetsConfig
    fields.foreach {
      case ff: FacetField[_] =>
        if ff.hierarchical then c.setHierarchical(ff.name, ff.hierarchical)
        if ff.multiValued then c.setMultiValued(ff.name, ff.multiValued)
        if ff.requireDimCount then c.setRequireDimCount(ff.name, ff.requireDimCount)
      case _ => // Ignore
    }
    c
  }
  private[lucene] lazy val hasFacets: Boolean = fields.exists(_.isInstanceOf[FacetField[_]])

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    this.path.foreach { path =>
      if Files.exists(path) then {
        val directory = FSDirectory.open(path)
        try {
          // A directory with no commit point yet (a process stopped before the first commit) holds no index to check.
          if DirectoryReader.indexExists(directory) then {
            val reader = DirectoryReader.open(directory)
            try reader.leaves().forEach { leaf =>
              val dataVersion = leaf.reader().asInstanceOf[SegmentReader].getSegmentInfo.info.getVersion
              val latest = Version.LATEST
              if latest != dataVersion then {
                // TODO: Support re-indexing
                scribe.warn(s"Data Version: $dataVersion, Latest Version: $latest")
              }
            } finally reader.close()
          }
        } finally directory.close()
      }
    }
  }).next(rebuildIfLeftBehind)

  // An index opened with its uncommitted marker down was left behind by a crash: rebuild it from its storage.
  private def rebuildIfLeftBehind: Task[Unit] = Task.defer {
    if !index.recoveredDirty then Task.unit
    else storeMode match {
      case StoreMode.Indexes(storage) =>
        val started = System.currentTimeMillis()
        logger.warn(s"$name: the search index was not durable when the database last stopped; rebuilding it from storage")
          .next(storage.init)
          .next(storage.transaction { stx =>
            transaction { tx =>
              tx.truncate.next(tx.insert(stx.stream))
            }
          })
          .flatMap { count =>
            Task(index.rebuilt())
              .next(logger.info(s"$name: search index rebuilt from $count stored documents in ${System.currentTimeMillis() - started} ms"))
          }
      case _ =>
        logger.warn(s"$name: the index has an uncommitted marker but no storage to rebuild from; it may be missing changes")
          .next(Task(index.rebuilt()))
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = storeMode match {
    case StoreMode.Indexes(storage) if parent.isEmpty =>
      storage.transaction.create().map { p =>
        LuceneTransaction(this, LuceneState[Doc](index, hasFacets), parent = Some(p), writeHandlerFactory = writeHandlerFactory, ownedParent = true)
      }
    case _ =>
      Task(LuceneTransaction(this, LuceneState[Doc](index, hasFacets), parent, writeHandlerFactory))
  }

  override def optimize(): Task[Unit] = Task {
    val s = index.createIndexSearcher()
    val currentSegments = try {
      s.getIndexReader.leaves().size()
    } finally {
      index.releaseIndexSearch(s)
    }
    scribe.info(s"Optimizing Lucene Index for $name. Current segment count: $currentSegments")
    index.write(_.forceMerge(1))
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    index.dispose()
  })
}

object LuceneStore extends CollectionManager {
  /** A manager whose stores use `durability` rather than the configured default ([[LuceneDurability.configured]]),
    * e.g. `SplitStoreManager(RocksDBStore, LuceneStore.withDurability(LuceneDurability.Deferred()))`. */
  def withDurability(durability: LuceneDurability): LuceneStoreManager = LuceneStoreManager(durability)

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = LuceneStore[Doc, Model]

  private val regexChars = ".?+*|{}[]()\"\\#~&<>@".toSet
  def escapeRegexLiteral(s: String): String = s.flatMap(c => if regexChars.contains(c) then s"\\$c" else c.toString)

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new LuceneStore[Doc, Model](name, path, model, storeMode, db, this)
}