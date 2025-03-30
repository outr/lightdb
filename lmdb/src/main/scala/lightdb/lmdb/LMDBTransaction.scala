package lightdb.lmdb

import lightdb.transaction.TransactionFeature
import rapid.Task

case class LMDBTransaction(instance: LMDBInstance) extends TransactionFeature {
  override def close(): Task[Unit] = instance.transactionManager.commit()
}
