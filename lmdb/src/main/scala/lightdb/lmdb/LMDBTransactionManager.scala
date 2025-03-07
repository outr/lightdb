package lightdb.lmdb

import org.lmdbjava._

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import rapid.Task
import lightdb.util.ActionIterator

case class LMDBTransactionManager(env: Env[ByteBuffer]) { manager =>
  @volatile private var writeTransaction: Txn[ByteBuffer] = _

  def withWrite[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] = Task.defer {
    manager.synchronized {
      if (writeTransaction == null) {
        writeTransaction = env.txnWrite()
      }
      f(writeTransaction)
    }
  }

  def commit(): Task[Unit] = Task {
    manager.synchronized {
      if (writeTransaction != null) {
        writeTransaction.commit()
        writeTransaction.close()
        writeTransaction = null
      }
    }
  }

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

  def get(dbi: Dbi[ByteBuffer], key: ByteBuffer): Task[Option[ByteBuffer]] = withWrite { txn =>
    Task {
      Option(dbi.get(txn, key)).filterNot(_.remaining() == 0)
    }
  }

  /** Count total records in the database */
  def count(dbi: Dbi[ByteBuffer]): Task[Int] = withWrite { txn =>
    Task(dbi.stat(txn).entries.toInt)
  }

  def withReadIterator[T](iteratorProvider: Txn[ByteBuffer] => Iterator[T]): Task[Iterator[T]] = Task {
    val txn = env.txnRead()

    val underlying = iteratorProvider(txn)

    ActionIterator(
      underlying,
      onNext = _ => (),
      onClose = () => txn.close()
    )
  }
}
