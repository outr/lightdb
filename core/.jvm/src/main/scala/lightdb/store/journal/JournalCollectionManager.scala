package lightdb.store.journal

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{CollectionManager, StoreMode}
import lightdb.util.Disposable
import rapid.Task

import java.nio.file.Path

/** [[JournalStoreManager]] for a manager whose stores are collections: queries go to the wrapped collections and read
  * their committed rows. */
case class JournalCollectionManager[M <: CollectionManager](wrapped: M,
                                                            recordBytes: Option[Long] = None,
                                                            defaultMaxOpBytes: Long = JournalStoreManager.DefaultMaxOpBytes,
                                                            maxOpBytes: Map[String, Long] = Map.empty,
                                                            retry: JournalRetry = JournalRetry(),
                                                            journalName: String = JournalStoreManager.DefaultName) extends CollectionManager with Disposable {
  lazy val journal: Journal = new Journal(wrapped, journalName, JournalStoreManager.recordBytes(wrapped, recordBytes), retry)

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = JournalCollection[Doc, Model]

  override def supportsJoin: Boolean = true

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new JournalCollection[Doc, Model](name, path, model, wrapped.create[Doc, Model](db, model, name, path, storeMode), journal,
      maxOpBytes.getOrElse(name, defaultMaxOpBytes), storeMode, db, this)

  override protected def doDispose(): Task[Unit] = journal.dispose.guarantee(wrapped match {
    case d: Disposable => d.dispose
    case _ => Task.unit
  })
}
