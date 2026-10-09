package lightdb.store.journal

import fabric.io.JsonFormatter
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.TransactionBoundExceededException
import lightdb.store.write.WriteOp
import lightdb.transaction.Transaction
import lightdb.transaction.handler.GroupCommitWriteHandler
import rapid.Task

import java.nio.charset.StandardCharsets

/**
 * The write buffer of a journalled transaction. A put larger than the store's declared op size fails loud at the
 * write. In a group (a group-commit transaction or one with joins) each write reserves the store's op size against
 * the record the intent must fit, and the write past it fails loud and aborts the group; a transaction with no group
 * commits what it holds as its own intent once it holds the store's op bound (not atomic past the bound).
 */
class JournalWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](transaction: Transaction[Doc, Model],
                                                                          store: JournalBacked[Doc, Model],
                                                                          commitPartial: List[WriteOp[Doc]] => Task[Unit])
  extends GroupCommitWriteHandler[Doc, Model] {
  override def write(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    op match {
      case WriteOp.Insert(doc) => checkSize(doc)
      case WriteOp.Upsert(doc) => checkSize(doc)
      case WriteOp.Delete(_) => ()
    }
    transaction.group match {
      case Some(group) =>
        val reserved = group.reservedBytes.addAndGet(store.maxOpBytes)
        if reserved > store.journal.maxRecordBytes then {
          val bound = (store.journal.maxRecordBytes / store.maxOpBytes).toInt
          val error = TransactionBoundExceededException(store.name, group.writeCount, bound)
          transaction.rollBackFor(error)
          group.owner.rollBackFor(error)
          throw error
        }
        super.write(op)
      case None =>
        super.write(op).next {
          if size >= store.opBound then commitPartial(drain()) else Task.unit
        }
    }
  }

  private def checkSize(doc: Doc): Unit = {
    val bytes = JsonFormatter.Compact(doc.json(store.model.rw)).getBytes(StandardCharsets.UTF_8).length
    if bytes > store.maxOpBytes then
      throw new IllegalArgumentException(s"A write to '${store.name}' of $bytes bytes is larger than the store's declared " +
        s"maximum op size of ${store.maxOpBytes} bytes")
  }
}
