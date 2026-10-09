package lightdb.store.journal

import lightdb.doc.{Document, DocumentModel}
import lightdb.progress.ProgressManager
import lightdb.store.Store
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.Task

/** A store made joinable by a [[Journal]]: its transactions buffer their writes, and its commits go through the
  * journal to `underlying`, a store of the wrapped manager. */
trait JournalBacked[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  val underlying: Store[Doc, Model]

  private[journal] def releaseReader(tx: Transaction[Doc, Model]): Task[Unit] =
    underlying.transaction.release(tx.asInstanceOf[underlying.TX])

  def journal: Journal

  /** The largest serialized document this store writes. A larger one fails loud at the write. */
  def maxOpBytes: Long

  /** The op bound of a group-commit transaction here: an intent of that many ops of [[maxOpBytes]] fits the record the
    * wrapped manager writes atomically, and it is never above [[Store.DefaultGroupCommitBound]]. */
  def opBound: Int = math.max(1L, math.min(Store.DefaultGroupCommitBound.toLong, journal.maxRecordBytes / maxOpBytes)).toInt

  override def defaultBatchConfig: BatchConfig = BatchConfig.StoreNative

  override def supportsGroupCommit: Boolean = true

  override def maxGroupCommitBound: Option[Int] = Some(opBound)

  override protected def joinRefusal(owner: Transaction[?, ?]): Option[String] = owner.store match {
    case s: JournalBacked[?, ?] if s.journal eq journal => None
    case s => Some(s"the owner's store ${s.name} is not journalled by journal ${journal.name}")
  }

  override protected def initialize(): Task[Unit] =
    underlying.init.next(journal.open).next(super.initialize())

  override def reIndex(progressManager: ProgressManager = ProgressManager.none,
                       commitEvery: Option[Int] = None): Task[Boolean] = underlying.reIndex(progressManager, commitEvery)

  override def optimize(): Task[Unit] = underlying.optimize()

  override protected def doDispose(): Task[Unit] =
    super.doDispose().guarantee(underlying.dispose).guarantee(journal.release(this))
}
