package lightdb.lmdb

import org.lmdbjava._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import rapid.Task
import lightdb.util.ActionIterator

case class LMDBTransactionManager(env: Env[ByteBuffer]) { manager =>
  private var lastCommit: Long = 0L
  private val activeWrites = new AtomicInteger(0)
  @volatile private var commitRequested = false
  @volatile private var writeTransaction: Txn[ByteBuffer] = _

  /** Get or create a new write transaction. Fails fast if commit is in progress. */
  private def getWrite(): Txn[ByteBuffer] = synchronized {
    if (commitRequested) {
      throw new IllegalStateException("A commit is already in progress. Cannot acquire a new write transaction.")
    }

    activeWrites.incrementAndGet()

    if (writeTransaction == null) {
      writeTransaction = env.txnWrite()
    } else {
    }

    writeTransaction
  }

  /** Release a write transaction safely */
  private def releaseWrite(txn: Txn[ByteBuffer]): Unit = synchronized {
    val remainingWrites = activeWrites.decrementAndGet()

    if (remainingWrites == 0 || commitRequested) {
      notifyAll()
    } else {
    }
  }

  /** Execute a function within a write transaction */
  def withWrite[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] = {
    Task.defer {
      val txn = getWrite()
      f(txn).guarantee(Task { releaseWrite(txn) })
    }
  }

  /** Commit the active write transaction safely */
  def commit(): Task[Unit] = Task {
    synchronized {
      if (writeTransaction == null) {
        return Task.unit
      }

      commitRequested = true

      val timeout = System.currentTimeMillis() + 5000 // 5s timeout
      var waitCount = 0
      while (activeWrites.get() > 0 && System.currentTimeMillis() < timeout) {
        waitCount += 1
        Thread.`yield`() // Allow other threads to proceed before waiting
        wait(100)
      }

      try {
        if (writeTransaction != null) {
          writeTransaction.commit()
          writeTransaction.close()
          writeTransaction = null
        }
        lastCommit = System.currentTimeMillis()
      } finally {
        commitRequested = false
        notifyAll()
      }
    }
  }

  /** Provide a read iterator wrapped in a write transaction */
  def withReadIterator[T](iteratorProvider: Txn[ByteBuffer] => Iterator[T]): Task[Iterator[T]] = Task {
    val txn = getWrite()

    val underlying = iteratorProvider(txn)

    ActionIterator(
      underlying,
      onNext = _ => (),
      onClose = () => releaseWrite(txn)
    )
  }

  /** Check if a key exists in the database */
  def exists(dbi: Dbi[ByteBuffer], key: ByteBuffer): Task[Boolean] = withWrite { txn =>
    Task {
      val cursor = dbi.openCursor(txn)
      try {
        cursor.get(key, GetOp.MDB_SET_KEY)
      } finally {
        cursor.close()
      }
    }
  }

  /** Retrieve a key-value from the database */
  def get(dbi: Dbi[ByteBuffer], key: ByteBuffer): Task[Option[ByteBuffer]] = withWrite { txn =>
    Task {
      Option(dbi.get(txn, key)).filterNot(_.remaining() == 0)
    }
  }

  /** Count total records in the database */
  def count(dbi: Dbi[ByteBuffer]): Task[Int] = withWrite { txn =>
    Task(dbi.stat(txn).entries.toInt)
  }
}
