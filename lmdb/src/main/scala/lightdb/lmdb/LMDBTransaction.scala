package lightdb.lmdb

import lightdb.transaction.TransactionFeature
import org.lmdbjava.Txn
import rapid.Task

import java.nio.ByteBuffer

case class LMDBTransaction(instance: LMDBInstance) extends TransactionFeature {
  override def close(): Task[Unit] = instance.transactionManager.commit()
}
