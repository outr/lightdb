package lightdb.lucene.index

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig, TieredMergePolicy}
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{BaseDirectory, ByteBuffersDirectory, FSDirectory}
import org.apache.lucene.util.IOUtils
import lightdb.lucene.LuceneDurability
import profig.Profig
import fabric.rw.*

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, LongAdder}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Callable, ExecutionException, Executors, ExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}

/**
 * A Lucene index and its single writer.
 *
 * @param durability when its committed changes become durable. Deferred applies only to an index on disk (`path`
 *                   set); the owning store decides whether the index qualifies ([[lightdb.lucene.LuceneStore]]).
 */
case class Index(path: Option[Path], durability: LuceneDurability = LuceneDurability.Immediate) {
  /** Whether durable commits are deferred: see [[LuceneDurability.Deferred]]. */
  val deferred: Boolean = path.nonEmpty && durability.isInstanceOf[LuceneDurability.Deferred]

  private val durableIntervalMs: Long = durability match {
    case LuceneDurability.Deferred(interval) => interval.toMillis
    case LuceneDurability.Immediate => 0L
  }

  lazy val analyzer: Analyzer = new StandardAnalyzer

  // IndexWriter operations must never run on a thread that can be
  // interrupted. Lucene's FSDirectory uses interruptible NIO channels, so a
  // `Thread.interrupt()` landing during a write/flush/commit closes the
  // channel and permanently breaks the writer — surfacing thereafter as
  // "FileLock invalidated by an external force" / "this IndexWriter is
  // closed". Effect runtimes that cancel a task by interrupting its carrier
  // thread (rapid's `Fiber.cancel`, for one) expose every write driven from a
  // cancellable context. Isolate all mutations + commit/rollback/dispose onto
  // a dedicated, never-interrupted thread: if the CALLER is interrupted while
  // awaiting the result the submitted op still runs to completion here, so the
  // writer is never left half-closed.
  private val writerExecutor: ExecutorService =
    Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"lucene-writer-${path.map(_.getFileName.toString).getOrElse("mem")}")
        t.setDaemon(true)
        t
      }
    })

  /** Run `f` on the dedicated writer thread, blocking the caller for the
    * result. An `ExecutionException` is unwrapped so the original failure
    * (e.g. a Lucene `IOException`) propagates unchanged. A caller interrupt
    * surfaces as `InterruptedException` while the submitted op completes
    * safely off-thread. */
  private def onWriter[T](f: => T): T =
    try writerExecutor.submit(new Callable[T] { override def call(): T = f }).get()
    catch {
      case e: ExecutionException => throw Option(e.getCause).getOrElse(e)
      case _: java.util.concurrent.RejectedExecutionException =>
        // The executor is shut down — `dispose` is in progress or done, and a
        // racing transaction release is still trying to flush. Interrupt
        // isolation no longer matters during teardown, so run inline (matches
        // the pre-isolation behavior: the op either succeeds or hits a closed
        // writer, rather than surfacing a spurious rejected-execution error).
        f
    }

  /** Route an `IndexWriter` mutation through the dedicated writer thread. */
  def write[T](f: IndexWriter => T): T = onWriter(f(indexWriter))

  private lazy val indexDirectory: BaseDirectory = path.map(FSDirectory.open).getOrElse(new ByteBuffersDirectory)
  // A fresh config per writer: Lucene refuses to reuse an IndexWriterConfig
  // once a writer has been built from it.
  private def newConfig: IndexWriterConfig = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(Profig("lightdb.lucene.ramBufferMB").opt[Double].getOrElse(2_000d))
    c.setMaxBufferedDocs(Profig("lightdb.lucene.maxBufferedDocs").opt[Int].getOrElse(10_000))
    c.setMergePolicy(new TieredMergePolicy)
    c.setMergeScheduler(new ConcurrentMergeScheduler)
    c.setUseCompoundFile(Profig("lightdb.lucene.useCompoundFile").opt[Boolean].getOrElse(false))
    c
  }

  // The writer and its NRT searcher manager are re-creatable, not fixed:
  // `IndexWriter.rollback()` is the only way to discard uncommitted
  // changes and it closes the writer for good, so a transaction rollback
  // used to leave every later write failing with "this IndexWriter is
  // closed" until the process restarted. A rollback now discards the
  // pending changes and the next access opens a new writer over the same
  // directory (the last commit is intact); the searcher manager is rebuilt
  // beside it because it is bound to the writer instance.
  private var currentWriter: IndexWriter = null
  private var currentSearchers: SearcherManager = null

  private def ensureWriter(): IndexWriter = synchronized {
    if currentWriter == null || !currentWriter.isOpen then {
      if currentWriter != null then writerLost()
      currentWriter = new IndexWriter(indexDirectory, newConfig)
      if currentSearchers != null then {
        try currentSearchers.close() catch { case _: Throwable => () }
      }
      currentSearchers = new SearcherManager(currentWriter, new SearcherFactory)
    }
    currentWriter
  }

  def indexWriter: IndexWriter = ensureWriter()
  private def searcherManager: SearcherManager = { ensureWriter(); currentSearchers }

  private lazy val taxonomyPath = path.map(p => p.resolve("taxonomy"))
  @volatile private var taxonomyLoaded = false
  private lazy val taxonomyDirectory: BaseDirectory = taxonomyPath.map { path =>
    if !Files.exists(path) then {
      Files.createDirectories(path)
    }
    taxonomyLoaded = true
    FSDirectory.open(path)
  }.getOrElse(new ByteBuffersDirectory)
  // Same shape for the taxonomy writer: its rollback closes it too.
  private var currentTaxonomyWriter: DirectoryTaxonomyWriter = null
  private var taxonomyWriterOpen = false

  def taxonomyWriter: DirectoryTaxonomyWriter = synchronized {
    if currentTaxonomyWriter == null || !taxonomyWriterOpen then {
      currentTaxonomyWriter = new DirectoryTaxonomyWriter(taxonomyDirectory)
      taxonomyWriterOpen = true
    }
    currentTaxonomyWriter
  }

  def createIndexSearcher(): IndexSearcher = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.acquire()
  }

  def createTaxonomyReader(): TaxonomyReader = new DirectoryTaxonomyReader(taxonomyWriter)

  def releaseIndexSearch(indexSearcher: IndexSearcher): Unit = searcherManager.release(indexSearcher)

  def releaseTaxonomyReader(taxonomyReader: TaxonomyReader): Unit = taxonomyReader.close()

  // Raw bodies. Kept separate so the public entry points can wrap them in
  // `onWriter` without the single-thread executor deadlocking on a
  // re-entrant submit. A commit also runs on a deferred-commit thread (see
  // below): IndexWriter commits concurrently with indexing.
  private def commitInternal(): Unit = {
    commits.increment()
    indexWriter.flush()
    indexWriter.commit()
    if taxonomyLoaded then {
      taxonomyWriter.commit()
    }
  }

  private def rollbackInternal(): Unit = {
    indexWriter.rollback()
    if taxonomyLoaded then {
      taxonomyWriter.rollback()
      taxonomyWriterOpen = false
    }
    // Both writers are now closed; `ensureWriter` / `taxonomyWriter` reopen
    // them on next use over the last committed state.
  }

  private val commits = new LongAdder

  /** How many Lucene commits (flush, commit point, fsync of the new files) this index has run. */
  def commitCount: Long = commits.sum()

  // -- Deferred durability ---------------------------------------------------------------------------------------
  //
  // The marker file is down whenever the index on disk may be behind what its storage holds: from before the first
  // change a transaction makes (and so before the change reaches storage) until a durable commit has covered every
  // published change with no transaction holding unpublished ones. It is fsynced when written, so the guarantee
  // survives an OS crash as well as a process kill; it is removed without one, which at worst costs a rebuild.

  private val marker: Option[Path] = path.map(_.resolve(Index.UncommittedMarker))

  /** Whether the index was opened with the marker down: a process stopped while its changes were not yet durable, so
    * the index may be missing writes its storage holds (or hold writes its storage rolled back). Its owner rebuilds
    * it from that storage, then calls [[rebuilt]]. */
  val recoveredDirty: Boolean = marker.exists(Files.exists(_))

  private val markerLock = new Object
  private var markerPresent = recoveredDirty
  // Kept down regardless of commits: the index must be rebuilt when next opened.
  private var rebuildRequired = recoveredDirty
  // Transactions that have begun changing the writer and not yet committed or rolled back.
  private val openChanges = new AtomicInteger(0)
  // Bumped by every committed transaction that changed the index.
  private val published = new AtomicLong(0L)
  private val durableScheduled = new AtomicBoolean(false)
  @volatile private var pendingDurable: ScheduledFuture[?] = null
  // Held through a deferred durable commit; disposal takes it to wait for a running one.
  private val durableLock = new ReentrantLock
  @volatile private var disposed = false

  private def ensureMarker(): Unit = marker.foreach { m =>
    if !markerPresent then {
      Files.createDirectories(m.getParent)
      Files.write(m, Array.emptyByteArray)
      IOUtils.fsync(m, false)
      IOUtils.fsync(m.getParent, true)
      markerPresent = true
    }
  }

  private def removeMarker(): Unit = marker.foreach { m =>
    if markerPresent then {
      Files.deleteIfExists(m)
      markerPresent = false
    }
  }

  /** Keep the marker down until the index is rebuilt, whatever commits follow. */
  def requireRebuild(): Unit = markerLock.synchronized {
    rebuildRequired = true
    ensureMarker()
  }

  /** The rebuild [[recoveredDirty]] or [[requireRebuild]] asked for is done: commit it durably and lift the marker. */
  def rebuilt(): Unit = {
    markerLock.synchronized { rebuildRequired = false }
    if deferred then commit()
    else {
      onWriter(commitInternal())
      markerLock.synchronized { removeMarker() }
    }
  }

  /** A transaction is about to change the index. Deferred: the marker goes down first, so a crash at any point after
    * this (its storage committed, the index not yet durable) leaves the index flagged for rebuild. */
  def beginChange(): Unit = if deferred then markerLock.synchronized {
    openChanges.incrementAndGet()
    ensureMarker()
  }

  private def endChange(): Unit = markerLock.synchronized {
    openChanges.decrementAndGet()
  }

  /** A transaction that changed the index committed. Immediate: commit durably before returning. Deferred: its
    * changes are already in the writer, where the next searcher opened sees them; schedule the durable commit. */
  def transactionCommitted(): Unit =
    if deferred then {
      published.incrementAndGet()
      endChange()
      scheduleDurable()
    } else onWriter(commitInternal())

  /** A transaction that changed the index rolled back. Immediate: discard the writer's uncommitted changes. Deferred:
    * the writer also holds other transactions' published changes, so the transaction has already restored what it
    * touched from storage ([[lightdb.lucene.LuceneTransaction]]) and only its change ends here. */
  def transactionRolledBack(): Unit =
    if deferred then endChange()
    else onWriter(rollbackInternal())

  /** Commit durably now: everything in the writer, published or not, becomes durable. */
  def commit(): Unit = if deferred then onWriter(commitDurable()) else onWriter(commitInternal())

  /** Discard the writer's uncommitted changes. Not for a deferred index: they include published changes. */
  def rollback(): Unit =
    if deferred then throw new UnsupportedOperationException(s"Cannot roll back the writer of a deferred-durability index (${path.getOrElse("memory")})")
    else onWriter(rollbackInternal())

  // One Lucene commit for every change published since the last one. The marker comes off only when nothing was
  // published after the commit began and no transaction holds unpublished changes.
  private def commitDurable(): Unit = {
    durableLock.lock()
    try {
      if !disposed then {
        durableScheduled.set(false)
        val upTo = published.get()
        commitInternal()
        markerLock.synchronized {
          if published.get() == upTo && openChanges.get() == 0 && !rebuildRequired then removeMarker()
        }
        if published.get() != upTo then scheduleDurable()
      }
    } finally durableLock.unlock()
  }

  private def scheduleDurable(): Unit =
    if !disposed && durableScheduled.compareAndSet(false, true) then {
      pendingDurable = Index.durableScheduler.schedule(new Runnable {
        override def run(): Unit =
          try commitDurable()
          catch {
            // The marker stays down: the next publish schedules another attempt, and an index that never manages
            // one is rebuilt when next opened.
            case t: Throwable => scribe.error(s"Deferred Lucene commit failed for ${path.getOrElse("memory")}", t)
          }
      }, durableIntervalMs, TimeUnit.MILLISECONDS)
    }

  // A deferred index's writer is closed only by disposal: a writer closed otherwise hit a tragic error, and the
  // published changes it held are gone. Its searches go on over the reopened writer, but the index is rebuilt when
  // next opened.
  private def writerLost(): Unit = if deferred && !disposed then {
    scribe.error(s"The Lucene writer for ${path.getOrElse("memory")} closed with changes not yet durable; the index will be rebuilt from storage when next opened")
    requireRebuild()
  }

  def dispose(): Unit = {
    if deferred then {
      durableLock.lock()
      try {
        disposed = true
        Option(pendingDurable).foreach(_.cancel(false))
      } finally durableLock.unlock()
    }
    onWriter {
      commitInternal()
      indexWriter.close()
      if taxonomyLoaded then {
        taxonomyWriterOpen = false
        taxonomyDirectory.close()
      }
    }
    if deferred then markerLock.synchronized {
      if openChanges.get() == 0 && !rebuildRequired then removeMarker()
    }
    writerExecutor.shutdown()
  }
}

object Index {
  /** The marker file a deferred-durability index keeps in its directory while it may be behind its storage. */
  val UncommittedMarker: String = ".lightdb-uncommitted"

  // Runs deferred durable commits, shared by every deferred index. Its threads are never interrupted (Lucene's
  // FSDirectory channels do not survive an interrupt): disposal cancels a pending commit without interrupting it and
  // waits for a running one.
  private lazy val durableScheduler: ScheduledThreadPoolExecutor = {
    val threads = math.max(1, math.min(4, Runtime.getRuntime.availableProcessors() / 2))
    val counter = new AtomicInteger(0)
    val s = new ScheduledThreadPoolExecutor(threads, new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"lucene-durable-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t
      }
    })
    s.setRemoveOnCancelPolicy(true)
    s
  }
}
