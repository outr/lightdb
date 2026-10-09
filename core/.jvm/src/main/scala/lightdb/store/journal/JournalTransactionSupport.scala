package lightdb.store.journal

import fabric.Json
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.Transaction
import lightdb.transaction.handler.GroupCommitWriteHandler
import rapid.Task

import java.util.concurrent.locks.ReentrantLock

/**
 * A transaction on a journalled store. Writes wait in its buffer and reach the wrapped store only through the journal
 * when its group commits. Reads by id see the buffer; scans, counts and queries read the wrapped store's committed
 * rows, so they see neither this transaction's buffered writes nor another's.
 */
trait JournalTransactionSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  def journalStore: JournalBacked[Doc, Model]

  @volatile private var truncatePending = false
  private val readerLock = new ReentrantLock()
  @volatile private var _reader: Transaction[Doc, Model] = null

  /** A transaction on the wrapped store, for reads; released with this one. */
  protected def reader: Task[Transaction[Doc, Model]] = Task.defer {
    journalStore.journal.checkReadable(journalStore.name)
    val existing = _reader
    if existing != null then Task.pure(existing)
    else Task.defer {
      readerLock.lock()
      try {
        if _reader == null then _reader = journalStore.underlying.transaction.withoutGroupCommit.create().sync()
        Task.pure(_reader)
      } finally readerLock.unlock()
    }
  }

  private def buffer: GroupCommitWriteHandler[Doc, Model] = writeHandler match {
    case g: GroupCommitWriteHandler[Doc, Model] @unchecked => g
    case other => throw new IllegalStateException(s"journalled transaction on ${journalStore.name} has write handler ${other.getClass.getName}")
  }

  private[journal] def takeEntry(): JournalEntry[Doc, Model] = {
    val entry = JournalEntry(journalStore, truncatePending, buffer.drain())
    truncatePending = false
    entry
  }

  private[journal] def trackCommitted(entry: JournalEntry[Doc, Model]): Unit = entry.ops.foreach {
    case WriteOp.Insert(doc) => trackCacheWrite(doc._id, Some(doc))
    case WriteOp.Upsert(doc) => trackCacheWrite(doc._id, Some(doc))
    case WriteOp.Delete(id) => trackCacheWrite(id, None)
  }

  /** Commits what a transaction with no group holds once it reaches the store's op bound. */
  private[journal] def commitPartial(ops: List[WriteOp[Doc]]): Task[Unit] = Task.defer {
    val entry = JournalEntry(journalStore, truncatePending, ops)
    truncatePending = false
    journalStore.journal.commit(List(entry)).map(_ => trackCommitted(entry))
  }

  override def jsonStream: rapid.Stream[Json] = rapid.Stream.force(reader.map(_.jsonStream))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] =
    if truncatePending then Task.pure(None)
    else if index == journalStore.idField then reader.flatMap(_.get(value.asInstanceOf[Id[Doc]]))
    else getByIndex(index, value)

  /** A read by a unique index other than the id; only a collection has one. */
  protected def getByIndex[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] =
    Task.error(new UnsupportedOperationException(s"${journalStore.name} can only get by _id, but ${index.name} was attempted"))

  override protected def _exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)

  override protected def _count: Task[Int] = reader.flatMap(_.count)

  override protected def _insert(doc: Doc): Task[Doc] = unbuffered

  override protected def _upsert(doc: Doc): Task[Doc] = unbuffered

  override protected def _delete(id: Id[Doc]): Task[Boolean] = unbuffered

  private def unbuffered[T]: Task[T] =
    Task.error(new UnsupportedOperationException(s"${journalStore.name} writes only through its journal"))

  // The owner commits the whole group: its own ops and every staged member's, as one intent.
  override protected def _commit: Task[Unit] = Task.defer {
    val joined: List[JournalTransactionSupport[?, ?]] = group.toList.flatMap(_.staged).collect { case m: JournalTransactionSupport[?, ?] => m }
    val members: List[JournalTransactionSupport[?, ?]] = this :: joined
    val entries: List[JournalEntry[?, ?]] = members.map(_.takeAnyEntry())
    journalStore.journal.commit(entries).map(_ => members.foreach(_.trackCommitting()))
  }

  @volatile private var committing: Option[JournalEntry[Doc, Model]] = None

  private[journal] def takeAnyEntry(): JournalEntry[?, ?] = {
    val entry = takeEntry()
    committing = Some(entry)
    entry
  }

  private[journal] def trackCommitting(): Unit = {
    committing.foreach(trackCommitted)
    committing = None
  }

  override protected def _rollback: Task[Unit] = Task {
    truncatePending = false
  }

  override protected def _close: Task[Unit] = Task.defer {
    readerLock.lock()
    val r = try {
      val r = _reader
      _reader = null
      r
    } finally readerLock.unlock()
    if r == null then Task.unit
    else journalStore.releaseReader(r)
  }

  /** Every row goes when the group commits, before this transaction's other writes; reads by id stop seeing what this
    * transaction had buffered. Returns the committed row count. */
  override def truncate: Task[Int] = reader.flatMap(_.count).flatMap { count =>
    buffer.clear.map { _ =>
      truncatePending = true
      count
    }
  }
}
