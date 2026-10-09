package lightdb.store

import lightdb.doc.{Document, DocumentModel}
import rapid.Task

/**
 * A store that can apply the writes of several stores in one atomic, durable write: every store whose
 * [[atomicGroup]] is the same object (for RocksDB, the stores of one database) can share it. A split collection over
 * such storage holds its group-commit transactions' writes and applies them, and those of the transactions joined to
 * them, in one such write.
 */
trait AtomicWriteStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  /** Stores with the same group can be written together by [[writeAtomically]]. */
  def atomicGroup: AnyRef

  /** Applies every batch in one atomic write, or none of them. An insert of an id that exists refuses the whole
    * write with a [[lightdb.error.DuplicateIdException]]. Every batch's store must share this store's group. */
  def writeAtomically(batches: List[AtomicBatch[?, ?]]): Task[Unit]
}
