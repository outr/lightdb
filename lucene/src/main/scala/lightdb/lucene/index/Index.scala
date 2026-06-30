package lightdb.lucene.index

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.facet.taxonomy.TaxonomyReader
import org.apache.lucene.facet.taxonomy.directory.{DirectoryTaxonomyReader, DirectoryTaxonomyWriter}
import org.apache.lucene.index.{ConcurrentMergeScheduler, IndexWriter, IndexWriterConfig, TieredMergePolicy}
import org.apache.lucene.search.{IndexSearcher, SearcherFactory, SearcherManager}
import org.apache.lucene.store.{BaseDirectory, ByteBuffersDirectory, FSDirectory}
import profig.Profig
import fabric.rw.*

import java.nio.file.{Files, Path}
import java.util.concurrent.{Callable, ExecutionException, Executors, ExecutorService, ThreadFactory}

case class Index(path: Option[Path]) {
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
  private lazy val config = {
    val c = new IndexWriterConfig(analyzer)
    c.setCommitOnClose(true)
    c.setRAMBufferSizeMB(Profig("lightdb.lucene.ramBufferMB").opt[Double].getOrElse(2_000d))
    c.setMaxBufferedDocs(Profig("lightdb.lucene.maxBufferedDocs").opt[Int].getOrElse(10_000))
    c.setMergePolicy(new TieredMergePolicy)
    c.setMergeScheduler(new ConcurrentMergeScheduler)
    c.setUseCompoundFile(Profig("lightdb.lucene.useCompoundFile").opt[Boolean].getOrElse(false))
    c
  }
  lazy val indexWriter = new IndexWriter(indexDirectory, config)
  private lazy val searcherManager = new SearcherManager(indexWriter, new SearcherFactory)

  private lazy val taxonomyPath = path.map(p => p.resolve("taxonomy"))
  private var taxonomyLoaded = false
  private lazy val taxonomyDirectory: BaseDirectory = taxonomyPath.map { path =>
    if !Files.exists(path) then {
      Files.createDirectories(path)
    }
    taxonomyLoaded = true
    FSDirectory.open(path)
  }.getOrElse(new ByteBuffersDirectory)
  lazy val taxonomyWriter: DirectoryTaxonomyWriter = new DirectoryTaxonomyWriter(taxonomyDirectory)

  def createIndexSearcher(): IndexSearcher = {
    searcherManager.maybeRefreshBlocking()
    searcherManager.acquire()
  }

  def createTaxonomyReader(): TaxonomyReader = new DirectoryTaxonomyReader(taxonomyWriter)

  def releaseIndexSearch(indexSearcher: IndexSearcher): Unit = searcherManager.release(indexSearcher)

  def releaseTaxonomyReader(taxonomyReader: TaxonomyReader): Unit = taxonomyReader.close()

  // Raw bodies (run ON the writer thread). Kept separate so the public
  // entry points can wrap them in `onWriter` without the single-thread
  // executor deadlocking on a re-entrant submit.
  private def commitInternal(): Unit = {
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
    }
  }

  def commit(): Unit = onWriter(commitInternal())

  def rollback(): Unit = onWriter(rollbackInternal())

  def dispose(): Unit = {
    onWriter {
      commitInternal()
      indexWriter.close()
      if taxonomyLoaded then {
        taxonomyDirectory.close()
      }
    }
    writerExecutor.shutdown()
  }
}