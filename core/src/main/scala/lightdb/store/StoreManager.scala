package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}

import java.nio.file.Path

trait StoreManager {
  lazy val name: String = getClass.getSimpleName.replace("$", "")

  type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: Store[Doc, Model]

  /**
   * Whether a transaction on one of this manager's stores can join a transaction on another of them
   * (`store.transaction.joining(owner)`), so that the owner's commit decides both. Native on PostgreSQL (the joined
   * transaction shares the owner's connection) and on a split collection over RocksDB (one `WriteBatch`); any other
   * manager gains it through [[lightdb.store.journal.JournalStoreManager]].
   */
  def supportsJoin: Boolean = false

  /** Whether this manager's stores write atomically together ([[AtomicWriteStore]]), so a split collection over them can
    * commit a group in one storage write. */
  def supportsAtomicWrites: Boolean = false

  /** The largest single record this manager's stores write atomically and durably, when it declares one. A journal
    * wrapper derives its op bound from it. */
  def maxRecordBytes: Option[Long] = None

  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                model: Model,
                                                                name: String,
                                                                path: Option[Path],
                                                                storeMode: StoreMode[Doc, Model]): S[Doc, Model]
}