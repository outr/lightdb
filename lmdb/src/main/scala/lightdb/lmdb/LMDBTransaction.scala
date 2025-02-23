package lightdb.lmdb

import lightdb.transaction.TransactionFeature
import org.lmdbjava.Txn
import rapid.Task

import java.nio.ByteBuffer

case class LMDBTransaction(txn: Txn[ByteBuffer]) extends TransactionFeature {
  override def commit(): Task[Unit] = Task {
    txn.commit()
  }

  override def rollback(): Task[Unit] = Task {
    txn.abort()
  }

  override def close(): Task[Unit] = Task {
    txn.commit()
    txn.close()
  }
}
