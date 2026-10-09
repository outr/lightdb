package lightdb.store.split

import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.progress.ProgressManager
import lightdb.error.StoreFailedException
import lightdb.id.Id
import lightdb.store.{AtomicWriteStore, Collection, Store, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.handler.GroupCommitWriteHandler
import lightdb.transaction.batch.BatchConfig
import rapid.{Task, logger, taskSeq2Ops}

import java.nio.file.Path
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

class SplitCollection[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model]
](override val name: String,
  path: Option[Path],
  model: Model,
  val storage: Storage,
  val searching: Searching,
  val storeMode: StoreMode[Doc, Model],
  db: LightDB,
  storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, db, storeManager) {
  override type TX = SplitCollectionTransaction[Doc, Model, Storage, Searching]

  override def defaultBatchConfig: BatchConfig = BatchConfig.StoreNative

  /**
   * Delegate native join support to the searching collection.
   *
   * This is critical when using SplitCollection as "system of record + derived search index", e.g.
   * RocksDB (storage) + OpenSearch (searching). Without this, ExistsChild will be forced through the planner
   * fallback (materialize parent ids), even though the searching backend can execute joins natively.
   */
  override def supportsNativeExistsChild: Boolean = searching.supportsNativeExistsChild

  override def supportsNestedQueries: Boolean = searching.supportsNestedQueries

  override protected def initialize(): Task[Unit] = storage.init.and(searching.init).next(super.initialize())

  // -- Group commit and joins ------------------------------------------------------------------------------------------

  private def atomicStorage: Option[AtomicWriteStore[Doc, Model]] = storage match {
    case a: AtomicWriteStore[Doc, Model] @unchecked => Some(a)
    case _ => None
  }

  /** Over storage that writes atomically (RocksDB), a group-commit transaction holds its writes and commits them in
    * one storage write. */
  override def supportsGroupCommit: Boolean = atomicStorage.nonEmpty

  /** A transaction here can join a group-commit transaction on a split collection whose storage shares this one's
    * atomic write (for RocksDB, the same database). */
  override protected def joinRefusal(owner: Transaction[?, ?]): Option[String] = atomicStorage match {
    case None => Some(s"$name's storage (${storage.storeManager.name}) cannot write atomically with another store")
    case Some(mine) => owner match {
      case o: SplitCollectionTransaction[?, ?, ?, ?] =>
        o.store.storage match {
          case theirs: AtomicWriteStore[?, ?] if theirs.atomicGroup eq mine.atomicGroup =>
            if o.isGroupCommit then None
            else Some(s"the transaction on ${o.store.name} writes through; only a group-commit transaction can be joined")
          case _ => Some(s"${o.store.name}'s storage is not in the same atomic write as $name's")
        }
      case o => Some(s"the owner's store ${o.store.name} is not a split collection")
    }
  }

  override protected def createWriteHandler(transaction: Transaction[Doc, Model], config: BatchConfig): WriteHandler[Doc, Model] =
    if transaction.isGroupCommit && supportsGroupCommit then new GroupCommitWriteHandler[Doc, Model]
    else super.createWriteHandler(transaction, config)

  // -- Index errors after a committed storage write ----------------------------------------------------------------------

  /** How rows whose index update failed after their storage write are re-indexed. */
  @volatile var indexRetry: IndexRetry = IndexRetry()

  // Each pending id with the generation of its latest failure, so a pass removes only what it indexed.
  private val pendingIndex = new ConcurrentHashMap[Id[Doc], java.lang.Long]()
  private val generations = new AtomicLong(0L)
  private val healing = new AtomicBoolean(false)
  private val indexedListeners = new CopyOnWriteArrayList[List[Id[Doc]] => Task[Unit]]()
  @volatile private var _indexFailure: Option[Throwable] = None

  /** Ids whose rows are committed in storage but not yet in the search index: a read by id finds them, a query does not
    * until the in-process re-index lands. */
  def indexPending: Set[Id[Doc]] = pendingIndex.keySet().asScala.toSet

  def isIndexPending(id: Id[Doc]): Boolean = pendingIndex.containsKey(id)

  /** Set once the in-process re-index, and then a full re-index, failed: every query fails loud until the database is
    * reopened, when the index is rebuilt from storage. */
  def indexFailure: Option[Throwable] = _indexFailure

  /** Called with the ids each in-process re-index lands. */
  def onIndexed(listener: List[Id[Doc]] => Task[Unit]): Unit = indexedListeners.add(listener)

  private[split] def checkIndex(): Unit = _indexFailure.foreach { t =>
    throw StoreFailedException(name, "its search index could not be repaired in process", t)
  }

  /** The storage write of `ids` committed but their index update failed: the commit stands, and the ids are re-indexed
    * in process, with the index marked behind as the crash backstop. */
  private[split] def indexFailed(ids: List[Id[Doc]], error: Throwable): Task[Unit] = Task.defer {
    scribe.error(s"$name: the search index update failed after its storage write committed; re-indexing ${ids.size} row(s) in process", error)
    val generation = generations.incrementAndGet()
    ids.foreach(id => pendingIndex.put(id, generation))
    searching.markIndexBehind().attempt.map { r =>
      r.failed.foreach(t => scribe.error(s"$name: could not mark the search index behind", t))
      scheduleHeal()
    }
  }

  private def scheduleHeal(): Unit =
    if healing.compareAndSet(false, true) then heal(1).handleError { t =>
      Task(scribe.error(s"$name: in-process re-index stopped", t))
    }.start()

  private def heal(attempt: Int): Task[Unit] = Task.sleep(indexRetry.delay(attempt)).next {
    val batch = pendingIndex.entrySet().asScala.toList.map(e => e.getKey -> e.getValue.longValue())
    reIndexIds(batch.map(_._1)).attempt.flatMap {
      case scala.util.Success(_) =>
        batch.foreach { case (id, generation) => pendingIndex.remove(id, generation) }
        notifyIndexed(batch.map(_._1)).next(finishHeal())
      case scala.util.Failure(error) if attempt < indexRetry.attempts =>
        logger.warn(s"$name: re-indexing ${batch.size} row(s) failed (attempt $attempt of ${indexRetry.attempts}): ${error.getMessage}")
          .next(heal(attempt + 1))
      case scala.util.Failure(error) =>
        logger.error(s"$name: re-indexing by id failed ${indexRetry.attempts} times; re-indexing the whole store", error)
          .next(reIndex(commitEvery = Some(1_000)).attempt).flatMap {
            case scala.util.Success(_) =>
              val all = pendingIndex.keySet().asScala.toList
              pendingIndex.clear()
              notifyIndexed(all).next(finishHeal())
            case scala.util.Failure(fatal) =>
              _indexFailure = Some(fatal)
              healing.set(false)
              logger.error(s"$name: the full re-index failed; queries fail until the database is reopened", fatal)
          }
    }
  }

  private def finishHeal(): Task[Unit] = Task.defer {
    healing.set(false)
    if pendingIndex.isEmpty then searching.markIndexCaughtUp()
    else Task(scheduleHeal())
  }

  private def notifyIndexed(ids: List[Id[Doc]]): Task[Unit] =
    if ids.isEmpty then Task.unit
    else indexedListeners.asScala.toList.foldLeft(Task.unit)((task, listener) =>
      task.next(Task.defer(listener(ids)).handleError(t => Task(scribe.error(s"$name: an indexed listener failed", t)))))

  // The index rows of `ids` rewritten from storage: upserted where storage holds the row, deleted where it does not.
  private def reIndexIds(ids: List[Id[Doc]]): Task[Unit] =
    if ids.isEmpty then Task.unit
    else storage.transaction { stx =>
      searching.transaction { itx =>
        ids.map(id => stx.get(id).flatMap {
          case Some(doc) => itx.upsert(doc).unit
          case None => itx.delete(id).unit
        }).tasks.unit
      }
    }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = for
    t <- Task(SplitCollectionTransaction(this, parent, writeHandlerFactory))
    t1 <- storage.transaction.withParent(t).withBatch(batchConfig).create()
    t2 <- searching.transaction.withParent(t).withBatch(batchConfig).create()
    _ = t._storage = t1.asInstanceOf[t.store.storage.TX]
    _ = t._searching = t2.asInstanceOf[t.store.searching.TX]
  yield t

  override def verify(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = if SplitCollection.ReIndexWhenOutOfSync then {
    transaction { transaction =>
      for
        storageCount <- transaction.storage.count
        searchCount <- transaction.searching.count
        shouldReIndex = storageCount != searchCount && model.fields.count(_.indexed) > 1
        _ <- logger.warn(s"$name out of sync! Storage Count: $storageCount, Search Count: $searchCount. Re-Indexing...")
          .next(reIndexInternal(transaction, progressManager))
          .next(logger.info(s"$name re-indexed successfully!"))
          .when(shouldReIndex)
      yield shouldReIndex
    }
  } else {
    Task.pure(false)
  }

  override def reIndex(progressManager: ProgressManager = ProgressManager.none,
                       commitEvery: Option[Int] = None): Task[Boolean] = transaction { transaction =>
    reIndexInternal(transaction, progressManager, commitEvery).map(_ => true)
  }

  override def reIndexDoc(doc: Doc): Task[Boolean] = transaction { transaction =>
    transaction.upsert(doc).map(_ => true)
  }

  override def optimize(): Task[Unit] = searching.optimize().next(storage.optimize())

  private def reIndexInternal(transaction: TX,
                              progressManager: ProgressManager,
                              commitEvery: Option[Int] = None): Task[Unit] = Task.defer {
    val startNanos = System.nanoTime()
    def elapsedMs: Long = (System.nanoTime() - startNanos) / 1_000_000L

    Task {
      scribe.debug(s"[reindex-trace] $name reIndexInternal starting (elapsedMs=$elapsedMs) ...")
    }.next {
      Task {
        scribe.debug(s"[reindex-trace] $name starting searching.truncate (elapsedMs=$elapsedMs) ...")
      }.next {
        transaction.searching.truncate
      }.flatMap { deleted =>
        Task {
          scribe.debug(s"[reindex-trace] $name searching.truncate completed deleted=$deleted (elapsedMs=$elapsedMs). Starting storage.count ...")
        }.next {
          transaction.storage.count
        }.flatMap { total =>
          Task {
            scribe.debug(s"[reindex-trace] $name storage.count=$total (elapsedMs=$elapsedMs). Starting searching.insert(stream) ...")
          }.next {
            val counter = new AtomicInteger(0)
            val LogEvery = 100_000
            transaction.searching.insert(
              transaction.storage.stream.evalTap { _ =>
                Task {
                  val count = counter.incrementAndGet()
                  if count == 1 || count % LogEvery == 0 || count == total then {
                    scribe.debug(s"[reindex-trace] $name streamed=$count/$total (elapsedMs=$elapsedMs)")
                  }
                  progressManager.percentage(
                    current = count,
                    total = total,
                    message = Some(s"Re-Indexing $name: $count of $total")
                  )
                }
              },
              commitEvery = commitEvery
            ).unit
          }.next(Task {
            scribe.debug(s"[reindex-trace] $name searching.insert(stream) completed (elapsedMs=$elapsedMs).")
          })
        }
      }.next(Task {
        scribe.debug(s"[reindex-trace] $name reIndexInternal finished (elapsedMs=$elapsedMs).")
      })
    }
  }

  override protected def doDispose(): Task[Unit] = storage.dispose.and(searching.dispose).next(super.doDispose())
}

object SplitCollection {
  var ReIndexWhenOutOfSync: Boolean = true
}