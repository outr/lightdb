package lightdb.store.journal

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{StoreManager, StoreMode}
import lightdb.util.Disposable
import rapid.Task

import java.nio.file.Path

/**
 * Makes any store manager's stores joinable: `store.transaction.joining(owner)` works on every store it creates, and
 * every commit goes through one [[Journal]] (buffered writes, one commit-intent record, then the apply, with unfinished
 * intents replayed at open). For a manager whose stores answer queries, use [[JournalCollectionManager]].
 *
 * @param recordBytes the largest single record `wrapped` writes atomically and durably; defaults to what it declares
 * @param maxOpBytes the largest serialized document a store writes, by store name, else `defaultMaxOpBytes`; a store's
 *                   group-commit op bound is `maxRecordBytes / maxOpBytes`, never above the default bound
 * @param journalName the name of the intent store on `wrapped`
 */
case class JournalStoreManager[M <: StoreManager](wrapped: M,
                                                  recordBytes: Option[Long] = None,
                                                  defaultMaxOpBytes: Long = JournalStoreManager.DefaultMaxOpBytes,
                                                  maxOpBytes: Map[String, Long] = Map.empty,
                                                  retry: JournalRetry = JournalRetry(),
                                                  journalName: String = JournalStoreManager.DefaultName) extends StoreManager with Disposable {
  lazy val journal: Journal = new Journal(wrapped, journalName, JournalStoreManager.recordBytes(wrapped, recordBytes), retry)

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = JournalStore[Doc, Model]

  override def supportsJoin: Boolean = true

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new JournalStore[Doc, Model](name, path, model, wrapped.create[Doc, Model](db, model, name, path, storeMode), journal,
      maxOpBytes.getOrElse(name, defaultMaxOpBytes), storeMode, db, this)

  override protected def doDispose(): Task[Unit] = journal.dispose.guarantee(wrapped match {
    case d: Disposable => d.dispose
    case _ => Task.unit
  })
}

object JournalStoreManager {
  val DefaultName: String = "_journal"
  val DefaultMaxOpBytes: Long = 64L * 1024L

  private[journal] def recordBytes(wrapped: StoreManager, declared: Option[Long]): Long =
    declared.orElse(wrapped.maxRecordBytes).getOrElse(throw new IllegalArgumentException(
      s"${wrapped.name} declares no maximum record size; give the journal wrapper one (recordBytes)"))
}
