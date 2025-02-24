package lightdb.lmdb

import org.lmdbjava._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import rapid.Task
import lightdb.util.ActionIterator

case class LMDBTransactionManager(env: Env[ByteBuffer]) {
  private val writeTxn = new AtomicReference[Option[ManagedTxn]](None)
  private val readTxn = new AtomicReference[Option[Txn[ByteBuffer]]](None)
  private val writeActive = new AtomicBoolean(false) // ✅ Ensures only one write txn

  /** Inner class to track commit state manually */
  private case class ManagedTxn(txn: Txn[ByteBuffer], var committed: Boolean = false)

  /** Safely execute a function within a **read transaction** */
  def withRead[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] = {
    Task.defer {
      val txn = readTxn.updateAndGet {
        case Some(activeTxn) => Some(activeTxn) // Reuse existing read transaction
        case None =>
          val newTxn = env.txnRead()
          Some(newTxn)
      }.get

      f(txn).guarantee(Task {
        if (readTxn.get().contains(txn)) {
          txn.close()
          readTxn.set(None)
        }
      })
    }
  }

  /** Provide an iterator wrapped in a read transaction */
  def withReadIterator[T](iteratorProvider: Txn[ByteBuffer] => Iterator[T]): Iterator[T] = {
    val txn = env.txnRead()
    readTxn.set(Some(txn))

    val underlying = iteratorProvider(txn)

    ActionIterator(
      underlying,
      onNext = _ => (), // No special action on next
      onClose = () => {
        if (readTxn.get().contains(txn)) {
          txn.close()
          readTxn.set(None)
        }
      }
    )
  }

  /** Safely execute a function within an **existing write transaction** or create one if needed */
  def withWrite[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] = {
    Task.defer {
      val managedTxn = writeTxn.updateAndGet {
        case Some(activeTxn) => Some(activeTxn) // ✅ Reuse active write transaction
        case None =>
          val newTxn = ManagedTxn(env.txnWrite())
          writeActive.set(true)
          Some(newTxn)
      }.get

      f(managedTxn.txn)
    }
  }

  /** Explicitly commit and close the active write transaction */
  def commit(): Task[Unit] = Task.defer {
    writeTxn.getAndSet(None) match {
      case Some(managedTxn) if !managedTxn.committed =>
        try {
          managedTxn.txn.commit()
          managedTxn.committed = true
          Task.unit
        } finally {
          managedTxn.txn.close()
          writeActive.set(false) // ✅ Release write lock
          refreshReadTxn() // ✅ Ensure fresh read transactions
        }
      case _ => Task.unit       // Ignore if there is no active transaction
    }
  }

  /** Close and refresh the read transaction after a write commit */
  private def refreshReadTxn(): Unit = {
    val txnOpt = readTxn.getAndSet(None)
    if (txnOpt.isDefined) {
      try {
        txnOpt.get.close()
      } catch {
        case _: Exception => println("Warning: Read transaction already closed!")
      }
    }
  }

  /** Explicitly close all transactions (e.g., on shutdown) */
  def close(): Unit = {
    writeTxn.getAndSet(None).foreach(_.txn.close())
    readTxn.getAndSet(None).foreach(_.close())
  }
}